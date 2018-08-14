#define RS_GC_C_

#include <math.h>
#include <assert.h>
#include <sys/param.h>
#include "inverted_index.h"
#include "redis_index.h"
#include "spec.h"
#include "redismodule.h"
#include "rmutil/util.h"
#include "gc.h"
#include "tests/time_sample.h"
#include "numeric_index.h"
#include "tag_index.h"
#include "config.h"
#include <unistd.h>
#include <sys/wait.h>
#include <stdbool.h>

uint32_t gcRevisionId = 0;

// convert a frequency to timespec
struct timespec hzToTimeSpec(float hz) {
  struct timespec ret;
  ret.tv_sec = 30;//(time_t)floor(1.0 / hz);
  ret.tv_nsec = 0;//(long)floor(1000000000.0 / hz) % 1000000000L;
  return ret;
}

typedef struct NumericFieldGCCtx {
  NumericRangeTree *rt;
  uint32_t revisionId;
  NumericRangeTreeIterator *gcIterator;
} NumericFieldGCCtx;

#define NUMERIC_GC_INITIAL_SIZE 4

#define SPEC_STATUS_OK 1
#define SPEC_STATUS_INVALID 2

/* Internal definition of the garbage collector context (each index has one) */
typedef struct GarbageCollectorCtx {

  // current frequency
  float hz;

  // inverted index key name for reopening the index
  const RedisModuleString *keyName;

  // periodic timer
  struct RMUtilTimer *timer;

  // statistics for reporting
  GCStats stats;

  // flag for rdb loading. Set to 1 initially, but unce it's set to 0 we don't need to check anymore
  int rdbPossiblyLoading;

  NumericFieldGCCtx **numericGCCtx;

  uint64_t spec_unique_id;

} GarbageCollectorCtx;

/* Create a new garbage collector, with a string for the index name, and initial frequency */
GarbageCollectorCtx *NewGarbageCollector(const RedisModuleString *k, float initialHZ,
                                         uint64_t spec_unique_id) {
  GarbageCollectorCtx *gc = malloc(sizeof(*gc));

  *gc = (GarbageCollectorCtx){
      .timer = NULL,
      .hz = initialHZ,
      .keyName = k,
      .stats = {0},
      .rdbPossiblyLoading = 1,
  };

  gc->numericGCCtx = array_new(NumericFieldGCCtx *, NUMERIC_GC_INITIAL_SIZE);
  gc->spec_unique_id = spec_unique_id;

  return gc;
}

/* Check if Redis is currently loading from RDB. Our thread starts before RDB loading is finished */
int isRdbLoading(RedisModuleCtx *ctx) {
  long long isLoading = 0;
  RMUtilInfo *info = RMUtil_GetRedisInfo(ctx);
  if (!info) {
    return 0;
  }

  if (!RMUtilInfo_GetInt(info, "loading", &isLoading)) {
    isLoading = 0;
  }

  RMUtilRedisInfo_Free(info);
  return isLoading == 1;
}

void gc_updateStats(RedisSearchCtx *sctx, GarbageCollectorCtx *gc, size_t recordsRemoved,
                    size_t bytesCollected) {
  sctx->spec->stats.numRecords -= recordsRemoved;
  sctx->spec->stats.invertedSize -= bytesCollected;
  gc->stats.totalCollected += bytesCollected;
}

size_t gc_RandomTerm(RedisModuleCtx *ctx, GarbageCollectorCtx *gc, int *status) {
  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName);
  size_t totalRemoved = 0;
  size_t totalCollected = 0;
  if (!sctx || sctx->spec->unique_id != gc->spec_unique_id) {
    RedisModule_Log(ctx, "warning", "No index spec for GC %s",
                    RedisModule_StringPtrLen(gc->keyName, NULL));
    *status = SPEC_STATUS_INVALID;
    goto end;
  }
  // Select a weighted random term
  TimeSample ts;
  char *term = IndexSpec_GetRandomTerm(sctx->spec, 20);
  // if the index is empty we won't get anything here
  if (!term) {
    goto end;
  }
  RedisModule_Log(ctx, "debug", "Garbage collecting for term '%s'", term);
  // Open the term's index
  InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, strlen(term), 1, &idxKey);
  if (idx) {
    int blockNum = 0;
    do {
      IndexRepairParams params = {.limit = RSGlobalConfig.gcScanSize};
      TimeSampler_Start(&ts);
      // repair 100 blocks at once
      blockNum = InvertedIndex_Repair(idx, &sctx->spec->docs, blockNum, &params);
      TimeSampler_End(&ts);
      RedisModule_Log(ctx, "debug", "Repair took %lldns", TimeSampler_DurationNS(&ts));
      /// update the statistics with the the number of records deleted
      totalRemoved += params.docsCollected;
      gc_updateStats(sctx, gc, params.docsCollected, params.bytesCollected);
      totalCollected += params.bytesCollected;
      // blockNum 0 means error or we've finished
      if (!blockNum) break;

      // After each iteration we yield execution
      // First we close the relevant keys we're touching
      RedisModule_CloseKey(idxKey);
      sctx = SearchCtx_Refresh(sctx, (RedisModuleString *)gc->keyName);
      // sctx null --> means it was deleted and we need to stop right now
      if (!sctx || sctx->spec->unique_id != gc->spec_unique_id) {
        *status = SPEC_STATUS_INVALID;
        break;
      }

      // reopen the inverted index - it might have gone away
      idx = Redis_OpenInvertedIndexEx(sctx, term, strlen(term), 1, &idxKey);
    } while (idx != NULL);
  }
  if (totalRemoved) {
    RedisModule_Log(ctx, "notice", "Garbage collected %zd bytes in %zd records for term '%s'",
                    totalCollected, totalRemoved, term);
  }
  free(term);
  RedisModule_Log(ctx, "debug", "New HZ: %f\n", gc->hz);
end:
  if (sctx) {
    RedisModule_CloseKey(sctx->key);
    SearchCtx_Free(sctx);
  }
  if (idxKey) RedisModule_CloseKey(idxKey);

  return totalRemoved;
}

static NumericRangeNode *NextGcNode(NumericFieldGCCtx *numericGcCtx) {
  bool runFromStart = false;
  NumericRangeNode *node = NULL;
  do {
    while ((node = NumericRangeTreeIterator_Next(numericGcCtx->gcIterator))) {
      if (node->range) {
        return node;
      }
    }
    assert(!runFromStart);
    NumericRangeTreeIterator_Free(numericGcCtx->gcIterator);
    numericGcCtx->gcIterator = NumericRangeTreeIterator_New(numericGcCtx->rt);
    runFromStart = true;
  } while (true);

  // will never reach here
  return NULL;
}

static NumericFieldGCCtx *gc_NewNumericGcCtx(NumericRangeTree *rt) {
  NumericFieldGCCtx *ctx = rm_malloc(sizeof(NumericFieldGCCtx));
  ctx->rt = rt;
  ctx->revisionId = rt->revisionId;
  ctx->gcIterator = NumericRangeTreeIterator_New(rt);
  return ctx;
}

static void gc_FreeNumericGcCtx(NumericFieldGCCtx *ctx) {
  NumericRangeTreeIterator_Free(ctx->gcIterator);
  rm_free(ctx);
}

static void gc_FreeNumericGcCtxArray(GarbageCollectorCtx *gc) {
  for (int i = 0; i < array_len(gc->numericGCCtx); ++i) {
    gc_FreeNumericGcCtx(gc->numericGCCtx[i]);
  }
  array_trimm_len(gc->numericGCCtx, 0);
}

static FieldSpec **getFieldsByType(IndexSpec *spec, FieldType type) {
#define FIELDS_ARRAY_CAP 2
  FieldSpec **fields = array_new(FieldSpec *, FIELDS_ARRAY_CAP);
  for (int i = 0; i < spec->numFields; ++i) {
    if (spec->fields[i].type == type) {
      fields = array_append(fields, &(spec->fields[i]));
    }
  }
  return fields;
}

static RedisModuleString *getRandomFieldByType(IndexSpec *spec, FieldType type) {
  FieldSpec **tagFields = NULL;
  tagFields = getFieldsByType(spec, type);
  if (array_len(tagFields) == 0) {
    array_free(tagFields);
    return NULL;
  }

  // choose random tag field
  int randomIndex = rand() % array_len(tagFields);

  RedisModuleString *ret = IndexSpec_GetFormattedKey(spec, tagFields[randomIndex]);
  array_free(tagFields);
  return ret;
}

size_t gc_TagIndex(RedisModuleCtx *ctx, GarbageCollectorCtx *gc, int *status) {
  size_t totalRemoved = 0;
  char *randomKey = NULL;
  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName);
  if (!sctx || sctx->spec->unique_id != gc->spec_unique_id) {
    RedisModule_Log(ctx, "warning", "No index spec for GC %s",
                    RedisModule_StringPtrLen(gc->keyName, NULL));
    *status = SPEC_STATUS_INVALID;
    goto end;
  }
  IndexSpec *spec = sctx->spec;

  RedisModuleString *keyName = getRandomFieldByType(spec, FIELD_TAG);
  if (!keyName) {
    goto end;
  }

  TagIndex *indexTag = TagIndex_Open(ctx, keyName, false, &idxKey);
  if (!indexTag) {
    goto end;
  }

  InvertedIndex *iv;
  tm_len_t len;

  if (!TrieMap_RandomKey(indexTag->values, &randomKey, &len, (void **)&iv)) {
    goto end;
  }

  int blockNum = 0;
  do {
    size_t bytesCollected = 0;
    size_t recordsRemoved = 0;
    // repair 100 blocks at once
    IndexRepairParams params = {.limit = RSGlobalConfig.gcScanSize, .arg = NULL};
    blockNum = InvertedIndex_Repair(iv, &sctx->spec->docs, blockNum, &params);
    /// update the statistics with the the number of records deleted
    totalRemoved += recordsRemoved;
    gc_updateStats(sctx, gc, recordsRemoved, bytesCollected);
    // blockNum 0 means error or we've finished
    if (!blockNum) break;

    // After each iteration we yield execution
    // First we close the relevant keys we're touching
    RedisModule_CloseKey(idxKey);
    sctx = SearchCtx_Refresh(sctx, (RedisModuleString *)gc->keyName);
    // sctx null --> means it was deleted and we need to stop right now
    if (!sctx || sctx->spec->unique_id != gc->spec_unique_id) {
      *status = SPEC_STATUS_INVALID;
      break;
    }

    // reopen inverted index
    indexTag = TagIndex_Open(ctx, keyName, false, &idxKey);
    if (!indexTag) {
      break;
    }
    iv = TrieMap_Find(indexTag->values, randomKey, len);
    if (iv == TRIEMAP_NOTFOUND) {
      break;
    }

  } while (true);

end:
  if (idxKey) RedisModule_CloseKey(idxKey);
  if (randomKey) {
    free(randomKey);
  }

  if (sctx) {
    RedisModule_CloseKey(sctx->key);
    SearchCtx_Free(sctx);
  }

  return totalRemoved;
}

size_t gc_NumericIndex(RedisModuleCtx *ctx, GarbageCollectorCtx *gc, int *status) {
  size_t totalRemoved = 0;
  RedisModuleKey *idxKey = NULL;
  FieldSpec **numericFields = NULL;
  RedisSearchCtx *sctx = NewSearchCtx(ctx, (RedisModuleString *)gc->keyName);
  if (!sctx || sctx->spec->unique_id != gc->spec_unique_id) {
    RedisModule_Log(ctx, "warning", "No index spec for GC %s",
                    RedisModule_StringPtrLen(gc->keyName, NULL));
    *status = SPEC_STATUS_INVALID;
    goto end;
  }
  IndexSpec *spec = sctx->spec;
  // find all the numeric fields
  numericFields = getFieldsByType(spec, FIELD_NUMERIC);

  if (array_len(numericFields) == 0) {
    goto end;
  }

  if (array_len(numericFields) != array_len(gc->numericGCCtx)) {
    // add all numeric fields to our gc
    assert(array_len(numericFields) >
           array_len(gc->numericGCCtx));  // it is not possible to remove fields
    gc_FreeNumericGcCtxArray(gc);
    for (int i = 0; i < array_len(numericFields); ++i) {
      RedisModuleString *keyName = IndexSpec_GetFormattedKey(spec, numericFields[i]);
      NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &idxKey);
      // if we could not open the numeric field we probably have a
      // corruption in our data, better to know it now.
      assert(rt);
      gc->numericGCCtx = array_append(gc->numericGCCtx, gc_NewNumericGcCtx(rt));
      if (idxKey) RedisModule_CloseKey(idxKey);
    }
  }

  // choose random numeric gc ctx
  int randomIndex = rand() % array_len(gc->numericGCCtx);
  NumericFieldGCCtx *numericGcCtx = gc->numericGCCtx[randomIndex];

  // open the relevent numeric index to check that our pointer is valid
  RedisModuleString *keyName = IndexSpec_GetFormattedKey(spec, numericFields[randomIndex]);
  NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &idxKey);
  if (idxKey) RedisModule_CloseKey(idxKey);

  if (numericGcCtx->rt != rt || numericGcCtx->revisionId != numericGcCtx->rt->revisionId) {
    // memory or revision changed, recreating our numeric gc ctx
    assert(numericGcCtx->rt != rt || numericGcCtx->revisionId < numericGcCtx->rt->revisionId);
    gc->numericGCCtx[randomIndex] = gc_NewNumericGcCtx(rt);
    gc_FreeNumericGcCtx(numericGcCtx);
    numericGcCtx = gc->numericGCCtx[randomIndex];
  }

  NumericRangeNode *nextNode = NextGcNode(numericGcCtx);

  int blockNum = 0;
  do {
    IndexRepairParams params = {.limit = RSGlobalConfig.gcScanSize, .arg = nextNode->range};
    // repair 100 blocks at once
    blockNum = InvertedIndex_Repair(nextNode->range->entries, &sctx->spec->docs, blockNum, &params);
    /// update the statistics with the the number of records deleted
    numericGcCtx->rt->numEntries -= params.docsCollected;
    totalRemoved += params.docsCollected;
    gc_updateStats(sctx, gc, params.docsCollected, params.bytesCollected);
    // blockNum 0 means error or we've finished
    if (!blockNum) break;

    sctx = SearchCtx_Refresh(sctx, (RedisModuleString *)gc->keyName);
    // sctx null --> means it was deleted and we need to stop right now
    if (!sctx || sctx->spec->unique_id != gc->spec_unique_id) {
      *status = SPEC_STATUS_INVALID;
      break;
    }
    if (numericGcCtx->revisionId != numericGcCtx->rt->revisionId) {
      break;
    }
  } while (true);

end:
  if (numericFields) {
    array_free(numericFields);
  }

  if (sctx) {
    RedisModule_CloseKey(sctx->key);
    SearchCtx_Free(sctx);
  }

  return totalRemoved;
}

typedef struct ForkGcCtx {
  const RedisModuleString *keyName;
  int pipefd[2];
  GarbageCollectorCtx *gc;
  uint64_t totalNodesInNumericTrees;
  uint64_t totalBlocksInNumericTrees;
  uint32_t lastRecievedGcRevisionId;
} ForkGcCtx;

void GC_StartForkGC(ForkGcCtx *gc);

/* The GC periodic callback, called in a separate thread. It selects a random term (using weighted
 * random) */
static int gc_periodicCallback(RedisModuleCtx *ctx, void *privdata) {
  GarbageCollectorCtx *gc = privdata;

  ForkGcCtx fgc;
  fgc.keyName = gc->keyName;
  fgc.gc = gc;
  fgc.totalBlocksInNumericTrees = 0;
  fgc.totalNodesInNumericTrees = 0;

  GC_StartForkGC(&fgc);
  return 1;

  int status = SPEC_STATUS_OK;
  RedisModule_AutoMemory(ctx);
  RedisModule_ThreadSafeContextLock(ctx);

  assert(gc);

  // Check if RDB is loading - not needed after the first time we find out that rdb is not reloading
  if (gc->rdbPossiblyLoading) {
    if (isRdbLoading(ctx)) {
      RedisModule_Log(ctx, "notice", "RDB Loading in progress, not performing GC");
      goto end;
    } else {
      // the RDB will not load again, so it's safe to ignore the info check in the next cycles
      gc->rdbPossiblyLoading = 0;
    }
  }

  size_t totalRemoved = 0;

  totalRemoved += gc_RandomTerm(ctx, gc, &status);

  totalRemoved += gc_NumericIndex(ctx, gc, &status);

  totalRemoved += gc_TagIndex(ctx, gc, &status);

  gc->stats.numCycles++;
  gc->stats.effectiveCycles += totalRemoved > 0 ? 1 : 0;

  // if we didn't remove anything - reduce the frequency a bit.
  // if we did  - increase the frequency a bit
  if (gc->timer) {
    // the timer is NULL if we've been cancelled
    if (totalRemoved > 0) {
      gc->hz = MIN(gc->hz * 1.2, GC_MAX_HZ);
    } else {
      gc->hz = MAX(gc->hz * 0.99, GC_MIN_HZ);
    }
    RMUtilTimer_SetInterval(gc->timer, hzToTimeSpec(gc->hz));
  }

end:

  RedisModule_ThreadSafeContextUnlock(ctx);

  return status == SPEC_STATUS_OK;
}

/* Termination callback for the GC. Called after we stop, and frees up all the resources. */
static void gc_onTerm(void *privdata) {
  GarbageCollectorCtx *gc = privdata;
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModule_ThreadSafeContextLock(ctx);
  RedisModule_FreeString(ctx, (RedisModuleString *)gc->keyName);
  for (int i = 0; i < array_len(gc->numericGCCtx); ++i) {
    gc_FreeNumericGcCtx(gc->numericGCCtx[i]);
  }
  array_free(gc->numericGCCtx);
  RedisModule_ThreadSafeContextUnlock(ctx);
  RedisModule_FreeThreadSafeContext(ctx);
  free(gc);
}

// Start the collector thread
int GC_Start(GarbageCollectorCtx *ctx) {
  assert(ctx->timer == NULL);
  ctx->timer = RMUtil_NewPeriodicTimer(gc_periodicCallback, gc_onTerm, ctx, hzToTimeSpec(ctx->hz));
  return REDISMODULE_OK;
}

/* Stop the garbage collector, and call its termination function asynchronously when its thread is
 * finished. This also frees the resources allocated for the GC context */
int GC_Stop(GarbageCollectorCtx *ctx) {
  if (ctx->timer) {
    RMUtilTimer_Terminate(ctx->timer);
    // set the timer to NULL so we won't call this twice
    ctx->timer = NULL;
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

// get the current stats from the collector
const GCStats *GC_GetStats(GarbageCollectorCtx *ctx) {
  if (!ctx) return NULL;
  return &ctx->stats;
}

// called externally when the user deletes a document to hint at increasing the HZ
void GC_OnDelete(GarbageCollectorCtx *ctx) {
  if (!ctx) return;
  ctx->hz = MIN(ctx->hz * 1.5, GC_MAX_HZ);
}

void GC_RenderStats(RedisModuleCtx *ctx, GarbageCollectorCtx *gc) {
#define REPLY_KVNUM(n, k, v)                   \
  RedisModule_ReplyWithSimpleString(ctx, k);   \
  RedisModule_ReplyWithDouble(ctx, (double)v); \
  n += 2

  int n = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  if (gc) {
    REPLY_KVNUM(n, "current_hz", gc->hz);
    REPLY_KVNUM(n, "bytes_collected", gc->stats.totalCollected);
    REPLY_KVNUM(n, "effectiv_cycles_rate",
                (double)gc->stats.effectiveCycles /
                    (double)(gc->stats.numCycles ? gc->stats.numCycles : 1));
    REPLY_KVNUM(n, "total_ms_run", gc->stats.totalMSRun);
    REPLY_KVNUM(n, "total_cycles", gc->stats.numCycles);
    REPLY_KVNUM(n, "avarage_cycle_time_ms", (double)gc->stats.totalMSRun / gc->stats.numCycles);
    REPLY_KVNUM(n, "last_run_time_ms", (double)gc->stats.lastRunTimeMs);
    REPLY_KVNUM(n, "num_of_numeric_nodes", (double)gc->stats.totalNodesInNumericTrees);
    REPLY_KVNUM(n, "total_blocks_in_numeric_trees", (double)gc->stats.totalBlocksInNumericTrees);
    REPLY_KVNUM(n, "total_empty_blocks_in_numeric_trees", (double)gc->stats.totalEmptyBlocksInNumericTrees);
    REPLY_KVNUM(n, "gc_numeric_trees_missed", (double)gc->stats.gcNumericNodesMissed);
    REPLY_KVNUM(n, "gc_blocks_denied", (double)gc->stats.gcBlockDenied);
  }
  RedisModule_ReplySetArrayLength(ctx, n);
}

static void GC_FDWriteLongLong(int fd, long long val){
  write(fd, &val, sizeof(long long));
}

static void GC_FDWriteBuffer(int fd, char* buff, size_t len){
  GC_FDWriteLongLong(fd, len);
  if(len > 0){
    write(fd, buff, len);
  }
}

static long long GC_FDReadLongLong(int fd){
  long long ret;
  ssize_t sizeRead = read(fd, &ret, sizeof(long long));
  if(sizeRead != sizeof(long long)){
    return 0;
  }
  return ret;
}

static char* GC_FDReadBuffer(int fd, size_t* len){
  *len = GC_FDReadLongLong(fd);
  if(*len == 0){
    return NULL;
  }
  char* buff = rm_malloc(*len * sizeof(char*));
  read(fd, buff, *len);
  return buff;
}

static bool GC_InvertedIndexRepair(ForkGcCtx *gc, RedisSearchCtx *sctx, InvertedIndex *idx,
                                   void (*RepairCallback)(const RSIndexResult *, void *),
                                   void* arg){
  int* blocksFixed = array_new(int, 10);
  int numDocsBefore[idx->size];
  long long totalBytesCollected = 0;
  long long totalDocsCollected = 0;
  for (uint32_t i = 0; i < idx->size ; ++i) {
    IndexBlock *blk = idx->blocks + i;
    if (blk->lastId - blk->firstId > UINT32_MAX) {
      // Skip over blocks which have a wide variation. In the future we might
      // want to split a block into two (or more) on high-delta boundaries.
      // todo: is it ok??
      continue;
    }
    IndexRepairParams params = {0};
    params.RepairCallback = RepairCallback;
    params.arg = arg;
    numDocsBefore[i] = blk->numDocs;
    int repaired = IndexBlock_Repair(blk, &sctx->spec->docs, idx->flags, &params);
    // We couldn't repair the block - return 0
    if (repaired == -1) {
      return false;
    }

    if (repaired > 0) {
      blocksFixed = array_append(blocksFixed, i);
    }

    totalBytesCollected += params.bytesCollected;
    totalDocsCollected += repaired;
  }

  if(array_len(blocksFixed) == 0){
    // no blocks was repaired
    GC_FDWriteLongLong(gc->pipefd[1], 0);
    array_free(blocksFixed);
    return false;
  }

  // write number of repaired blocks
  GC_FDWriteLongLong(gc->pipefd[1], array_len(blocksFixed));

  // write total bytes collected
  GC_FDWriteLongLong(gc->pipefd[1], totalBytesCollected);

  // write total docs collected
  GC_FDWriteLongLong(gc->pipefd[1], totalDocsCollected);

  // write total number of blocks
  GC_FDWriteLongLong(gc->pipefd[1], idx->size);

  for(int i = 0 ; i < array_len(blocksFixed) ; ++i){
    // write fix block
    IndexBlock *blk = idx->blocks + blocksFixed[i];
    GC_FDWriteLongLong(gc->pipefd[1], blocksFixed[i]); // writing the block index
    GC_FDWriteLongLong(gc->pipefd[1], blk->firstId);
    GC_FDWriteLongLong(gc->pipefd[1], blk->lastId);
    GC_FDWriteLongLong(gc->pipefd[1], blk->numDocs);
    GC_FDWriteLongLong(gc->pipefd[1], numDocsBefore[blocksFixed[i]]); // send num docs before
    if(blk->data->data){
      GC_FDWriteBuffer(gc->pipefd[1], blk->data->data, blk->data->cap);
      GC_FDWriteLongLong(gc->pipefd[1], blk->data->offset);
    }else{
      GC_FDWriteBuffer(gc->pipefd[1], NULL, 0);
      GC_FDWriteLongLong(gc->pipefd[1], 0);
    }
  }
  array_free(blocksFixed);
  return true;
}

static void GC_CollectTerm(ForkGcCtx *gc, RedisSearchCtx *sctx, char* term, size_t termLen){
  RedisModuleKey *idxKey = NULL;
  InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, strlen(term), 1, &idxKey);
  if (idx) {
    // inverted index name
    GC_FDWriteBuffer(gc->pipefd[1], term, termLen);

    GC_InvertedIndexRepair(gc, sctx, idx, NULL, NULL);
  }
  if(idxKey){
    RedisModule_CloseKey(idxKey);
  }
}

static void GC_CountDeletedCardinality(const RSIndexResult *r, void* arg){
  CardinalityValue *valuesDeleted = arg;
  for(int i = 0 ; i < array_len(valuesDeleted); ++i){
    if(valuesDeleted[i].value == r->num.value){
      valuesDeleted[i].appearances++;
      return;
    }
  }
}

static void GC_CollectGarbage(ForkGcCtx *gc){
  RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NewSearchCtx(rctx, (RedisModuleString *)gc->keyName);
  size_t totalRemoved = 0;
  size_t totalCollected = 0;
  if (!sctx || sctx->spec->unique_id != gc->gc->spec_unique_id) {
    // write log here
    RedisModule_FreeThreadSafeContext(rctx);
    return;
  }
  // Select a weighted random term
  TimeSample ts;

  TrieIterator *iter = Trie_Iterate(sctx->spec->terms, "", 0, 0, 1);
  rune *rstr = NULL;
  t_len slen = 0;
  float score = 0;
  int dist = 0;
  while (TrieIterator_Next(iter, &rstr, &slen, NULL, &score, &dist)) {
    size_t termLen;
    char *term = runesToStr(rstr, slen, &termLen);
    GC_CollectTerm(gc, sctx, term, termLen);
    free(term);
  }
  DFAFilter_Free(iter->ctx);
  free(iter->ctx);
  TrieIterator_Free(iter);

  // we are done with terms
  GC_FDWriteBuffer(gc->pipefd[1], "\0", 1);

  // moving to numeric fields
  FieldSpec **numericFields = getFieldsByType(sctx->spec, FIELD_NUMERIC);

  if (array_len(numericFields) != 0) {
    for(int i = 0 ; i < array_len(numericFields) ; ++i){
      RedisModuleString *keyName = IndexSpec_GetFormattedKey(sctx->spec, numericFields[i]);
      NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &idxKey);

      NumericRangeTreeIterator *gcIterator = NumericRangeTreeIterator_New(rt);
      NumericRangeNode *currNode = NULL;

      uint64_t numericNodes = 0;
      // numeric field name
      GC_FDWriteBuffer(gc->pipefd[1], numericFields[i]->name, strlen(numericFields[i]->name));
      // numeric field unique id
      GC_FDWriteLongLong(gc->pipefd[1], rt->uniqueId);

      while((currNode = NumericRangeTreeIterator_Next(gcIterator))){
        ++numericNodes;
        if(!currNode->range){
          continue;
        }

        CardinalityValue *valuesDeleted = array_new(CardinalityValue, currNode->range->card);
        for(int i = 0 ; i < currNode->range->card ; ++i){
          CardinalityValue valueDeleted;
          valueDeleted.value = currNode->range->values[i].value;
          valueDeleted.appearances = 0;
          valuesDeleted = array_append(valuesDeleted, valueDeleted);
        }
        // write node pointer
        GC_FDWriteLongLong(gc->pipefd[1], (long long)currNode);

        bool repaired = GC_InvertedIndexRepair(gc, sctx, currNode->range->entries, GC_CountDeletedCardinality, valuesDeleted);

        if(repaired){
          //send reduced cardinality size
          GC_FDWriteLongLong(gc->pipefd[1], currNode->range->card);

          // send reduced cardinality
          for(int i = 0 ; i < currNode->range->card ; ++i){
            GC_FDWriteLongLong(gc->pipefd[1], valuesDeleted[i].appearances);
          }
        }
        array_free(valuesDeleted);
      }

      // we are done with the current field
      GC_FDWriteLongLong(gc->pipefd[1], 0);

      // sending number of numeric nodes
      GC_FDWriteLongLong(gc->pipefd[1], numericNodes);

      if (idxKey) RedisModule_CloseKey(idxKey);

      NumericRangeTreeIterator_Free(gcIterator);
    }
  }

  // we are done with numeric fields
  GC_FDWriteBuffer(gc->pipefd[1], "\0", 1);


  if (sctx) {
    RedisModule_CloseKey(sctx->key);
    SearchCtx_Free(sctx);
    RedisModule_FreeThreadSafeContext(rctx);
  }
}

typedef struct ModifiedBlock{
  long long blockIndex;
  int numBlocksBefore;
  IndexBlock blk;
}ModifiedBlock;

typedef struct{
  long long bytesCollected;
  long long docsCollected;
  ModifiedBlock* blocksModified;
}GC_InvertedIndexData;

static void GC_ReadModifiedBlock(ForkGcCtx *gc, ModifiedBlock* blockModified){
  blockModified->blockIndex = GC_FDReadLongLong(gc->pipefd[0]);
  blockModified->blk.firstId = GC_FDReadLongLong(gc->pipefd[0]);
  blockModified->blk.lastId = GC_FDReadLongLong(gc->pipefd[0]);
  blockModified->blk.numDocs = GC_FDReadLongLong(gc->pipefd[0]);
  blockModified->numBlocksBefore = GC_FDReadLongLong(gc->pipefd[0]);
  size_t cap;
  char* data = GC_FDReadBuffer(gc->pipefd[0], &cap);
  blockModified->blk.data = malloc(sizeof(Buffer));
  blockModified->blk.data->offset = GC_FDReadLongLong(gc->pipefd[0]);;
  blockModified->blk.data->cap = cap;
  blockModified->blk.data->data = data;
// todo: think what to do with this statistics
  if(data == NULL){
    gc->gc->stats.totalEmptyBlocksInNumericTrees++;
  }
}

static bool GC_ReadInvertedIndexFromFork(ForkGcCtx *gc, GC_InvertedIndexData* idxData){
  long long blocksModifiedSize = GC_FDReadLongLong(gc->pipefd[0]);
  if(blocksModifiedSize == 0){
    return false;
  }

  idxData->bytesCollected = GC_FDReadLongLong(gc->pipefd[0]);
  idxData->docsCollected = GC_FDReadLongLong(gc->pipefd[0]);
  GC_FDReadLongLong(gc->pipefd[0]); //throw totalblocks in inverted index

  idxData->blocksModified = array_new(ModifiedBlock, blocksModifiedSize);
  for(int i = 0 ; i < blocksModifiedSize ; ++i){
    GC_ReadModifiedBlock(gc, idxData->blocksModified + i);
  }
  return true;
}

static void GC_FixInvertedIndex(GC_InvertedIndexData* idxData, InvertedIndex *idx){
  for(int i = 0 ; i < array_len(idxData->blocksModified) ; ++i){
    if(idxData->blocksModified[i].numBlocksBefore == idx->blocks[idxData->blocksModified[i].blockIndex].numDocs){
      indexBlock_Free(&idx->blocks[idxData->blocksModified[i].blockIndex]);
      idx->blocks[idxData->blocksModified[i].blockIndex].data = idxData->blocksModified[i].blk.data;
      idx->blocks[idxData->blocksModified[i].blockIndex].firstId = idxData->blocksModified[i].blk.firstId;
      idx->blocks[idxData->blocksModified[i].blockIndex].lastId = idxData->blocksModified[i].blk.lastId;
      idx->blocks[idxData->blocksModified[i].blockIndex].numDocs = idxData->blocksModified[i].blk.numDocs;
    }else{
      Buffer_Free(idxData->blocksModified[i].blk.data);
    }
  }
}

bool GC_ReadInvertedIndex(ForkGcCtx *gc){
  size_t len;
  char* term = GC_FDReadBuffer(gc->pipefd[0], &len);
  if(term == NULL || term[0] == '\0'){
    if(term){
      rm_free(term);
    }
    return false;
  }

  GC_InvertedIndexData idxData = {0};
  if(!GC_ReadInvertedIndexFromFork(gc, &idxData)){
    rm_free(term);
    return true;
  }

  RedisModuleCtx *rctx = NULL;
  rctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModule_ThreadSafeContextLock(rctx);
  RedisModuleKey *idxKey = NULL;
  RedisSearchCtx *sctx = NULL;
  sctx = NewSearchCtx(rctx, (RedisModuleString *)gc->keyName);
  if (!sctx || sctx->spec->unique_id != gc->gc->spec_unique_id) {
    goto cleanup;
  }

  InvertedIndex *idx = Redis_OpenInvertedIndexEx(sctx, term, strlen(term), 1, &idxKey);

  if(idx == NULL){
    goto cleanup;
  }

  GC_FixInvertedIndex(&idxData, idx);

  gc_updateStats(sctx, gc->gc, idxData.docsCollected, idxData.bytesCollected);

cleanup:
  if(rctx){
    RedisModule_ThreadSafeContextUnlock(rctx);
  }

  if(idxKey){
    RedisModule_CloseKey(idxKey);
  }
  if (sctx) {
    RedisModule_CloseKey(sctx->key);
    SearchCtx_Free(sctx);
  }
  if(term){
    rm_free(term);
  }
  if(rctx){
    RedisModule_FreeThreadSafeContext(rctx);
  }
  if(idxData.blocksModified){
    array_free(idxData.blocksModified);
  }

  return true;
}

bool GC_ReadNumericInvertedIndex(ForkGcCtx *gc){
  size_t fieldNameLen;
  char* fieldName = GC_FDReadBuffer(gc->pipefd[0], &fieldNameLen);
  if(fieldName == NULL || fieldName[0] == '\0'){
    if(fieldName){
      rm_free(fieldName);
    }
    return false;
  }

  uint64_t rtUniqueId = GC_FDReadLongLong(gc->pipefd[0]);

  RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(NULL);
  NumericRangeNode *currNode = NULL;
  bool shouldReturn = false;
  while((currNode = (NumericRangeNode *)GC_FDReadLongLong(gc->pipefd[0]))){

// performs cleanup and return
#define RETURN shouldReturn = true; goto loop_cleanup;
// performs cleanup and continue with the loop
#define CONTINUE goto loop_cleanup;

    GC_InvertedIndexData idxData = {0};
    if(!GC_ReadInvertedIndexFromFork(gc, &idxData)){
      continue;
    }

    // read reduced cardinality size
    long long reduceCardinalitySize = GC_FDReadLongLong(gc->pipefd[0]);
    long long valuesDeleted[reduceCardinalitySize];

    // read reduced cardinality
    for(int i = 0 ; i < reduceCardinalitySize ; ++i){
      valuesDeleted[i] = GC_FDReadLongLong(gc->pipefd[0]);
    }

    RedisModule_ThreadSafeContextLock(rctx);
    RedisSearchCtx *sctx = NewSearchCtx(rctx, (RedisModuleString *)gc->keyName);
    if (!sctx || sctx->spec->unique_id != gc->gc->spec_unique_id) {
      RETURN;
    }

    RedisModuleString *keyName = fmtRedisNumericIndexKey(sctx, fieldName);
    RedisModuleKey *idxKey = NULL;
    NumericRangeTree *rt = OpenNumericIndex(sctx, keyName, &idxKey);

    if(rt->uniqueId != rtUniqueId){
      RETURN;
    }


    if(!currNode->range){
      gc->gc->stats.gcNumericNodesMissed++; // todo: think if its needed here
      CONTINUE;
    }

    GC_FixInvertedIndex(&idxData, currNode->range->entries);

    // fixing cardinality
    uint16_t newCard = 0;
    CardinalityValue* newCardValues = array_new(CardinalityValue, currNode->range->splitCard);
    for(int i = 0 ; i < array_len(currNode->range->values) ; ++i){
      int appearances = currNode->range->values[i].appearances;
      if(i < reduceCardinalitySize){
        appearances -= valuesDeleted[i];
      }
      if(appearances > 0){
        CardinalityValue val;
        val.value = currNode->range->values[i].value;
        val.appearances = appearances;
        newCardValues = array_append(newCardValues, val);
        ++newCard;
      }
    }
    array_free(currNode->range->values);
    newCardValues = array_trimm_cap(newCardValues, newCard);
    currNode->range->values = newCardValues;
    currNode->range->card = newCard;

loop_cleanup:
    RedisModule_ThreadSafeContextUnlock(rctx);
    if (sctx) {
      RedisModule_CloseKey(sctx->key);
      SearchCtx_Free(sctx);
    }
    if(idxData.blocksModified){
      array_free(idxData.blocksModified);
    }
    if(keyName){
      RedisModule_FreeString(rctx, keyName);
    }
    if(idxKey){
      RedisModule_CloseKey(idxKey);
    }
    if(shouldReturn){
      RedisModule_FreeThreadSafeContext(rctx);
      return false;
    }
  }

  // read number of numeric nodes
  gc->totalNodesInNumericTrees += GC_FDReadLongLong(gc->pipefd[0]); // think if we need this here

  RedisModule_FreeThreadSafeContext(rctx);

  if(fieldName){
    rm_free(fieldName);
  }
  return true;

}

void GC_ReadInvertedIndexes(ForkGcCtx *gc){
  while (GC_ReadInvertedIndex(gc));
  while (GC_ReadNumericInvertedIndex(gc));
}

void GC_StartForkGC(ForkGcCtx *gc){
  pid_t cpid;
  TimeSample ts;

  size_t totalCollectedBefore = gc->gc->stats.totalCollected;

  TimeSampler_Start(&ts);
  pipe(gc->pipefd); // create the pipe
  cpid = fork(); // duplicate the current process
  if (cpid == 0)
  {
    close(gc->pipefd[0]);
    GC_CollectGarbage(gc);
    close(gc->pipefd[1]);
    _exit(EXIT_SUCCESS);
  }
  else
  {
    close(gc->pipefd[1]);
    GC_ReadInvertedIndexes(gc);
    close(gc->pipefd[0]);
    wait(NULL);
  }
  TimeSampler_End(&ts);

  long long msRun = TimeSampler_DurationMS(&ts);

  gc->gc->stats.numCycles++;
  gc->gc->stats.totalMSRun += msRun;
  gc->gc->stats.lastRunTimeMs = msRun;

  // todo: handle updating the timer
  // if we didn't remove anything - reduce the frequency a bit.
  // if we did  - increase the frequency a bit
//  if (gc->gc->timer) {
//    // the timer is NULL if we've been cancelled
//    if (totalCollectedBefore < gc->gc->stats.totalCollected) {
//      gc->gc->hz = MIN(gc->gc->hz * 10, GC_MAX_HZ);
//    } else {
//      gc->gc->hz = MAX(gc->gc->hz * 0.1, GC_MIN_HZ);
//    }
//    RMUtilTimer_SetInterval(gc->gc->timer, hzToTimeSpec(gc->gc->hz));
//  }


  // todo: think if its needed here
  if(gc->gc->stats.totalNodesInNumericTrees < gc->totalNodesInNumericTrees){
    gc->gc->stats.totalNodesInNumericTrees = gc->totalNodesInNumericTrees;
  }
  if(gc->gc->stats.totalBlocksInNumericTrees < gc->totalBlocksInNumericTrees){
    gc->gc->stats.totalBlocksInNumericTrees = gc->totalBlocksInNumericTrees;
  }

}

