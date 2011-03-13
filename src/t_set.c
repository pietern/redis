#include "redis.h"

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/

/* Factory method to return a set that *can* hold "value". When the object has
 * an integer-encodable value, an intset will be returned. Otherwise a regular
 * hash table. */
robj *setTypeCreate(robj *value) {
    if (isObjectRepresentableAsLongLong(value,NULL) == REDIS_OK)
        return createIntsetObject();
    return createSetObject();
}

int tsetAddLiteral(robj *sobj, rlit *elelit) {
    robj *eleobj;
    long long llval;
    uint8_t success = 0;

    if (sobj->encoding == REDIS_ENCODING_INTSET) {
        if (litGetLongLong(elelit,&llval)) {
            sobj->ptr = intsetAdd(sobj->ptr,llval,&success);
            if (success) {
                /* Convert when it contains too many entries. */
                if (intsetLen(sobj->ptr) > server.set_max_intset_entries)
                    tsetConvert(sobj,REDIS_ENCODING_HT);
                return 1;
            }
        } else {
            /* Failed to get integer from object, convert to regular set. */
            tsetConvert(sobj,REDIS_ENCODING_HT);

            /* The set *was* an intset and this value is not integer
             * encodable, so dictAdd should always work. */
            eleobj = litGetObject(elelit);
            redisAssert(dictAdd(sobj->ptr,eleobj,NULL) == DICT_OK);
            incrRefCount(eleobj);
            return 1;
        }
    } else if (sobj->encoding == REDIS_ENCODING_HT) {
        eleobj = litGetObject(elelit);
        if (dictAdd(sobj->ptr,eleobj,NULL) == DICT_OK) {
            incrRefCount(eleobj);
            return 1;
        }
    } else {
        redisPanic("Unknown set encoding");
    }

    return 0;
}

int tsetAddObject(robj *sobj, robj *eleobj) {
    rlit elelit;
    litFromObject(&elelit,eleobj);

    /* No need to clear dirty literal since it is created from an object. */
    return tsetAddLiteral(sobj,&elelit);
}

int tsetRemoveLiteral(robj *sobj, rlit *elelit) {
    robj *eleobj;
    long long llval;
    int success = 0;

    if (sobj->encoding == REDIS_ENCODING_INTSET) {
        /* Only integer values can be removed from an intset. */
        if (litGetLongLong(elelit,&llval)) {
            sobj->ptr = intsetRemove(sobj->ptr,llval,&success);
            if (success) return 1;
        }
    } else if (sobj->encoding == REDIS_ENCODING_HT) {
        eleobj = litGetObject(elelit);
        if (dictDelete(sobj->ptr,eleobj) == DICT_OK) {
            if (htNeedsResize(sobj->ptr)) dictResize(sobj->ptr);
            return 1;
        }
    } else {
        redisPanic("Unknown set encoding");
    }

    return 0;
}

int tsetRemoveObject(robj *sobj, robj *eleobj) {
    rlit elelit;
    litFromObject(&elelit,eleobj);

    /* No need to clear dirty literal since it is created from an object. */
    return tsetRemoveLiteral(sobj,&elelit);
}

void tsetRandomElement(robj *sobj, rlit *lit) {
    if (sobj->encoding == REDIS_ENCODING_INTSET) {
        litFromLongLong(lit,intsetRandom(sobj->ptr));
    } else if (sobj->encoding == REDIS_ENCODING_HT) {
        dictEntry *de = dictGetRandomKey(sobj->ptr);
        litFromObject(lit,dictGetEntryKey(de));
    } else {
        redisPanic("Unknown set encoding");
    }
}

unsigned int tsetSize(robj *sobj) {
    redisAssert(sobj->type == REDIS_SET);
    if (sobj->encoding == REDIS_ENCODING_INTSET) {
        return intsetLen(sobj->ptr);
    } else if (sobj->encoding == REDIS_ENCODING_HT) {
        return dictSize((dict*)sobj->ptr);
    } else {
        redisPanic("Unknown set encoding");
    }

    return 0; /* Avoid warnings. */
}

int tsetFindLiteral(robj *sobj, rlit *elelit) {
    robj *eleobj;
    long long llval;

    if (sobj->encoding == REDIS_ENCODING_INTSET) {
        return litGetLongLong(elelit,&llval) && intsetFind(sobj->ptr,llval);
    } else if (sobj->encoding == REDIS_ENCODING_HT) {
        eleobj = litGetObject(elelit);
        return dictFind(sobj->ptr,eleobj) != NULL;
    } else {
        redisPanic("Unknown set encoding");
    }

    return 0; /* Avoid warnings. */
}

int tsetFindObject(robj *sobj, robj *eleobj) {
    rlit elelit;
    litFromObject(&elelit,eleobj);

    /* No need to clear dirty literal since it is created from an object. */
    return tsetFindLiteral(sobj,&elelit);
}

void tsetInitIterator(iterset *it, robj *sobj) {
    redisAssert(sobj->type == REDIS_SET);
    it->encoding = sobj->encoding;
    if (it->encoding == REDIS_ENCODING_INTSET) {
        it->iter.is.is = sobj->ptr;
        it->iter.is.ii = 0;
    } else if (it->encoding == REDIS_ENCODING_HT) {
        it->iter.ht.dict = sobj->ptr;
        it->iter.ht.di = dictGetIterator(it->iter.ht.dict);
        it->iter.ht.de = dictNext(it->iter.ht.di);
    } else {
        redisPanic("Unknown set encoding");
    }
}

int tsetNext(iterset *it, rlit *ele) {
    if (it->encoding == REDIS_ENCODING_INTSET) {
        long long ll;
        if (!intsetGet(it->iter.is.is,it->iter.is.ii,&ll)) return 0;
        litFromLongLong(ele,ll);

        /* Move to next element. */
        it->iter.is.ii++;
    } else if (it->encoding == REDIS_ENCODING_HT) {
        if (it->iter.ht.de == NULL) return 0;
        litFromObject(ele,(robj*)dictGetEntryKey(it->iter.ht.de));

        /* Move to next element. */
        it->iter.ht.de = dictNext(it->iter.ht.di);
    } else {
        redisPanic("Unknown set encoding");
    }

    return 1;
}

void tsetClearIterator(iterset *it) {
    if (it->encoding == REDIS_ENCODING_INTSET) {
        /* skip */
    } else if (it->encoding == REDIS_ENCODING_HT) {
        dictReleaseIterator(it->iter.ht.di);
    } else {
        redisPanic("Unknown set encoding");
    }
}

/* Convert set to specified encoding. When converting to a hash table, the dict
 * is presized to hold the number of elements in the original set. */
void tsetConvert(robj *sobj, int encoding) {
    redisAssert(sobj->type == REDIS_SET);
    if (sobj->encoding == REDIS_ENCODING_INTSET) {
        iterset it;
        rlit ele;
        dict *dict = dictCreate(&setDictType,NULL);

        if (encoding != REDIS_ENCODING_HT)
            redisPanic("Unknown target encoding");

        /* Presize the dict to avoid rehashing */
        dictExpand(dict,intsetLen(sobj->ptr));

        tsetInitIterator(&it,sobj);
        while (tsetNext(&it,&ele)) {
            robj *tmp = litGetObject(&ele);
            incrRefCount(tmp);
            redisAssert(dictAdd(dict,tmp,NULL) == DICT_OK);
            litClearDirtyObject(&ele);
        }
        tsetClearIterator(&it);

        zfree(sobj->ptr);
        sobj->encoding = REDIS_ENCODING_HT;
        sobj->ptr = dict;
    } else if (sobj->encoding == REDIS_ENCODING_HT) {
        redisPanic("Unsupported set conversion");
    } else {
        redisPanic("Unknown set encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/

void saddCommand(redisClient *c) {
    robj *set;

    set = lookupKeyWrite(c->db,c->argv[1]);
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    if (set == NULL) {
        set = setTypeCreate(c->argv[2]);
        dbAdd(c->db,c->argv[1],set);
    } else {
        if (set->type != REDIS_SET) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
    }
    if (tsetAddObject(set,c->argv[2])) {
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
        addReply(c,shared.cone);
    } else {
        addReply(c,shared.czero);
    }
}

void sremCommand(redisClient *c) {
    robj *set;

    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    c->argv[2] = tryObjectEncoding(c->argv[2]);
    if (tsetRemoveObject(set,c->argv[2])) {
        if (tsetSize(set) == 0) dbDelete(c->db,c->argv[1]);
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
        addReply(c,shared.cone);
    } else {
        addReply(c,shared.czero);
    }
}

void smoveCommand(redisClient *c) {
    robj *srcset, *dstset, *ele;
    srcset = lookupKeyWrite(c->db,c->argv[1]);
    dstset = lookupKeyWrite(c->db,c->argv[2]);
    ele = c->argv[3] = tryObjectEncoding(c->argv[3]);

    /* If the source key does not exist return 0 */
    if (srcset == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    if (checkType(c,srcset,REDIS_SET) ||
        (dstset && checkType(c,dstset,REDIS_SET))) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    if (srcset == dstset) {
        addReply(c,shared.cone);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    if (!tsetRemoveObject(srcset,ele)) {
        addReply(c,shared.czero);
        return;
    }

    /* Remove the src set from the database when empty */
    if (tsetSize(srcset) == 0) dbDelete(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[2]);
    server.dirty++;

    /* Create the destination set when it doesn't exist */
    if (!dstset) {
        dstset = setTypeCreate(ele);
        dbAdd(c->db,c->argv[2],dstset);
    }

    /* An extra key has changed when ele was successfully added to dstset */
    if (tsetAddObject(dstset,ele)) server.dirty++;
    addReply(c,shared.cone);
}

void sismemberCommand(redisClient *c) {
    robj *set;

    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    if (tsetFindObject(set,c->argv[2]))
        addReply(c,shared.cone);
    else
        addReply(c,shared.czero);
}

void scardCommand(redisClient *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_SET)) return;

    addReplyLongLong(c,tsetSize(o));
}

void spopCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *sobj;
    robj *eleobj;
    rlit elelit;

    if ((sobj = lookupKeyWriteOrReply(c,key,shared.nullbulk)) == NULL ||
        checkType(c,sobj,REDIS_SET)) return;

    tsetRandomElement(sobj,&elelit);
    eleobj = litGetObject(&elelit);

    /* When the literal is created from an actual object, it might be destroyed
     * when removing it from the set (since the dictionary does decrRefCount on
     * removed elements). We need it later on, so protect it with a refcount. */
    incrRefCount(eleobj);
    redisAssert(tsetRemoveLiteral(sobj,&elelit));

    /* We own a reference, so the literal can be cleared. */
    litClearDirtyObject(&elelit);

    /* Change argv to replicate as SREM */
    c->argc = 3;
    c->argv = zrealloc(c->argv,sizeof(robj*)*(c->argc));

    /* Overwrite SREM with SPOP (same length) */
    redisAssert(sdslen(c->argv[0]->ptr) == 4);
    memcpy(c->argv[0]->ptr, "SREM", 4);

    /* Popped element already has incremented refcount */
    c->argv[2] = eleobj;

    addReplyBulk(c,eleobj);
    if (tsetSize(sobj) == 0) dbDelete(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

void srandmemberCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *sobj;
    rlit elelit;

    if ((sobj = lookupKeyReadOrReply(c,key,shared.nullbulk)) == NULL ||
        checkType(c,sobj,REDIS_SET)) return;

    tsetRandomElement(sobj,&elelit);
    addReplyBulkLiteral(c,&elelit);
}

int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    return tsetSize(*(robj**)s1)-tsetSize(*(robj**)s2);
}

void sinterGenericCommand(redisClient *c, robj **setkeys, unsigned long setnum, robj *dstkey) {
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    robj *dstset = NULL;
    iterset it;
    rlit ele;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;

    for (j = 0; j < setnum; j++) {
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            zfree(sets);
            if (dstkey) {
                if (dbDelete(c->db,dstkey)) {
                    signalModifiedKey(c->db,dstkey);
                    server.dirty++;
                }
                addReply(c,shared.czero);
            } else {
                addReply(c,shared.emptymultibulk);
            }
            return;
        }
        if (checkType(c,setobj,REDIS_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }
    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performace */
    qsort(sets,setnum,sizeof(robj*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    if (!dstkey) {
        replylen = addDeferredMultiBulkLength(c);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        dstset = createIntsetObject();
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    tsetInitIterator(&it,sets[0]);
    while (tsetNext(&it,&ele)) {
        for (j = 1; j < setnum; j++) {
            if (!tsetFindLiteral(sets[j],&ele))
                break;
        }

        /* Add element to reply or dst set when present in all sets. */
        if (j == setnum) {
            if (!dstkey) {
                addReplyBulkLiteral(c,&ele);
                cardinality++;
            } else {
                tsetAddLiteral(dstset,&ele);
            }
        }

        /* Clean up object if it was created in the mean time. */
        litClearDirtyObject(&ele);
    }
    tsetClearIterator(&it);

    if (dstkey) {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */
        dbDelete(c->db,dstkey);
        if (tsetSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,tsetSize(dstset));
        } else {
            decrRefCount(dstset);
            addReply(c,shared.czero);
        }
        signalModifiedKey(c->db,dstkey);
        server.dirty++;
    } else {
        setDeferredMultiBulkLength(c,replylen,cardinality);
    }
    zfree(sets);
}

void sinterCommand(redisClient *c) {
    sinterGenericCommand(c,c->argv+1,c->argc-1,NULL);
}

void sinterstoreCommand(redisClient *c) {
    sinterGenericCommand(c,c->argv+2,c->argc-2,c->argv[1]);
}

#define REDIS_OP_UNION 0
#define REDIS_OP_DIFF 1
#define REDIS_OP_INTER 2

void sunionDiffGenericCommand(redisClient *c, robj **setkeys, int setnum, robj *dstkey, int op) {
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    robj *dstset = NULL;
    iterset it;
    rlit ele;
    int j, cardinality = 0;

    for (j = 0; j < setnum; j++) {
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            sets[j] = NULL;
            continue;
        }
        if (checkType(c,setobj,REDIS_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key*/
    dstset = createIntsetObject();

    /* Iterate all the elements of all the sets, add every element a single
     * time to the result set */
    for (j = 0; j < setnum; j++) {
        if (op == REDIS_OP_DIFF && j == 0 && !sets[j]) break; /* result set is empty */
        if (!sets[j]) continue; /* non existing keys are like empty sets */

        tsetInitIterator(&it,sets[j]);
        while (tsetNext(&it,&ele)) {
            if (op == REDIS_OP_UNION || j == 0) {
                if (tsetAddLiteral(dstset,&ele))
                    cardinality++;
            } else if (op == REDIS_OP_DIFF) {
                if (tsetRemoveLiteral(dstset,&ele))
                    cardinality--;
            } else {
                redisPanic("Unknown set op");
            }

            /* Clean up object if it was created in the mean time. */
            litClearDirtyObject(&ele);
        }
        tsetClearIterator(&it);

        /* Exit when result set is empty. */
        if (op == REDIS_OP_DIFF && cardinality == 0) break;
    }

    /* Output the content of the resulting set, if not in STORE mode */
    if (!dstkey) {
        tsetInitIterator(&it,dstset);
        addReplyMultiBulkLen(c,cardinality);
        while (tsetNext(&it,&ele))
            addReplyBulkLiteral(c,&ele);
        tsetClearIterator(&it);
        decrRefCount(dstset);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        dbDelete(c->db,dstkey);
        if (tsetSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,tsetSize(dstset));
        } else {
            decrRefCount(dstset);
            addReply(c,shared.czero);
        }
        signalModifiedKey(c->db,dstkey);
        server.dirty++;
    }
    zfree(sets);
}

void sunionCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,REDIS_OP_UNION);
}

void sunionstoreCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],REDIS_OP_UNION);
}

void sdiffCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,REDIS_OP_DIFF);
}

void sdiffstoreCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],REDIS_OP_DIFF);
}
