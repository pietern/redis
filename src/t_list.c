#include "redis.h"

/*-----------------------------------------------------------------------------
 * List API
 *----------------------------------------------------------------------------*/

/* Check the argument length to see if it requires us to convert the ziplist
 * to a real list. Only check raw-encoded objects because integer encoded
 * objects are never too long. */
void tlistTryConversion(robj *subject, robj *value) {
    if (subject->encoding != REDIS_ENCODING_ZIPLIST) return;
    if (value->encoding == REDIS_ENCODING_RAW &&
        sdslen(value->ptr) > server.list_max_ziplist_value)
            tlistConvert(subject,REDIS_ENCODING_LINKEDLIST);
}

void tlistPush(robj *subject, robj *value, int where) {
    /* Check if we need to convert the ziplist */
    tlistTryConversion(subject,value);
    if (subject->encoding == REDIS_ENCODING_ZIPLIST &&
        ziplistLen(subject->ptr) >= server.list_max_ziplist_entries)
            tlistConvert(subject,REDIS_ENCODING_LINKEDLIST);

    if (subject->encoding == REDIS_ENCODING_ZIPLIST) {
        int pos = (where == REDIS_HEAD) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
        value = getDecodedObject(value);
        subject->ptr = ziplistPush(subject->ptr,value->ptr,sdslen(value->ptr),pos);
        decrRefCount(value);
    } else if (subject->encoding == REDIS_ENCODING_LINKEDLIST) {
        if (where == REDIS_HEAD) {
            listAddNodeHead(subject->ptr,value);
        } else {
            listAddNodeTail(subject->ptr,value);
        }
        incrRefCount(value);
    } else {
        redisPanic("Unknown list encoding");
    }
}

robj *tlistPop(robj *subject, int where) {
    robj *value = NULL;
    if (subject->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *p;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        int pos = (where == REDIS_HEAD) ? 0 : -1;
        p = ziplistIndex(subject->ptr,pos);
        if (ziplistGet(p,&vstr,&vlen,&vlong)) {
            if (vstr) {
                value = createStringObject((char*)vstr,vlen);
            } else {
                value = createStringObjectFromLongLong(vlong);
            }
            /* We only need to delete an element when it exists */
            subject->ptr = ziplistDelete(subject->ptr,&p);
        }
    } else if (subject->encoding == REDIS_ENCODING_LINKEDLIST) {
        list *list = subject->ptr;
        listNode *ln;
        if (where == REDIS_HEAD) {
            ln = listFirst(list);
        } else {
            ln = listLast(list);
        }
        if (ln != NULL) {
            value = listNodeValue(ln);
            incrRefCount(value);
            listDelNode(list,ln);
        }
    } else {
        redisPanic("Unknown list encoding");
    }
    return value;
}

unsigned int tlistLength(robj *lobj) {
    redisAssert(lobj->type == REDIS_LIST);
    if (lobj->encoding == REDIS_ENCODING_ZIPLIST) {
        return ziplistLen(lobj->ptr);
    } else if (lobj->encoding == REDIS_ENCODING_LINKEDLIST) {
        return listLength((list*)lobj->ptr);
    } else {
        redisPanic("Unknown list encoding");
    }

    return 0; /* Avoid warnings. */
}

void tlistInitIterator(iterlist *it, robj *lobj) {
    redisAssert(lobj->type == REDIS_LIST);
    it->encoding = lobj->encoding;
    if (it->encoding == REDIS_ENCODING_ZIPLIST) {
        it->iter.zl.zl = lobj->ptr;
        it->iter.zl.eptr = ziplistIndex(it->iter.zl.zl,0);
    } else if (it->encoding == REDIS_ENCODING_LINKEDLIST) {
        it->iter.dll.list = lobj->ptr;
        it->iter.dll.ln = listFirst(it->iter.dll.list);
    } else {
        redisPanic("Unknown list encoding");
    }
}

int tlistNext(iterlist *it, rlit *ele) {
    if (it->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *str = NULL;
        unsigned int len;
        long long ll;

        if (it->iter.zl.eptr == NULL)
            return 0;

        redisAssert(ziplistGet(it->iter.zl.eptr,&str,&len,&ll));
        if (str != NULL)
            litFromBuffer(ele,(char*)str,(int)len);
        else
            litFromLongLong(ele,ll);

        /* Move to next element. */
        it->iter.zl.eptr = ziplistNext(it->iter.zl.zl,it->iter.zl.eptr);
    } else if (it->encoding == REDIS_ENCODING_LINKEDLIST) {
        if (it->iter.dll.ln == NULL)
            return 0;

        litFromObject(ele,listNodeValue(it->iter.dll.ln));

        /* Move to next element. */
        it->iter.dll.ln =  it->iter.dll.ln->next;
    } else {
        redisPanic("Unknown list encoding");
    }

    return 1;
}

void tlistClearIterator(iterlist *it) {
    REDIS_NOTUSED(it);
    /* Nothing to clear. */
}

void tlistConvert(robj *lobj, int encoding) {
    redisAssert(lobj->type == REDIS_LIST);
    if (lobj->encoding == encoding) return;
    if (lobj->encoding == REDIS_ENCODING_ZIPLIST) {
        iterlist it;
        rlit ele;
        list *list;

        if (encoding != REDIS_ENCODING_LINKEDLIST)
            redisPanic("Unknown target encoding");

        list = listCreate();
        listSetFreeMethod(list,decrRefCount);

        tlistInitIterator(&it,lobj);
        while (tlistNext(&it,&ele)) {
            robj *tmp = litGetObject(&ele);
            incrRefCount(tmp);
            listAddNodeTail(list,tmp);
            litClearDirtyObject(&ele);
        }
        tlistClearIterator(&it);

        zfree(lobj->ptr);
        lobj->encoding = REDIS_ENCODING_LINKEDLIST;
        lobj->ptr = list;
    } else if (lobj->encoding == REDIS_ENCODING_LINKEDLIST) {
        redisPanic("Unsupported list conversion");
    } else {
        redisPanic("Unknown list encoding");
    }
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/

void pushGenericCommand(redisClient *c, int where) {
    robj *lobj = lookupKeyWrite(c->db,c->argv[1]);
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    if (lobj == NULL) {
        if (handleClientsWaitingListPush(c,c->argv[1],c->argv[2])) {
            addReply(c,shared.cone);
            return;
        }
        lobj = createZiplistObject();
        dbAdd(c->db,c->argv[1],lobj);
    } else {
        if (lobj->type != REDIS_LIST) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
        if (handleClientsWaitingListPush(c,c->argv[1],c->argv[2])) {
            signalModifiedKey(c->db,c->argv[1]);
            addReply(c,shared.cone);
            return;
        }
    }
    tlistPush(lobj,c->argv[2],where);
    addReplyLongLong(c,tlistLength(lobj));
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

void lpushCommand(redisClient *c) {
    pushGenericCommand(c,REDIS_HEAD);
}

void rpushCommand(redisClient *c) {
    pushGenericCommand(c,REDIS_TAIL);
}

void pushxGenericCommand(redisClient *c, robj *refval, robj *val, int where) {
    robj *key = c->argv[1];
    robj *lobj;
    int inserted = 0;

    if ((lobj = lookupKeyReadOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,lobj,REDIS_LIST)) return;

    if (refval != NULL) {
        /* Note: we expect refval to be string-encoded because it is *not* the
         * last argument of the multi-bulk LINSERT. */
        redisAssert(refval->encoding == REDIS_ENCODING_RAW);

        /* We're not sure if this value can be inserted yet, but we cannot
         * convert the list inside the iterator. We don't want to loop over the
         * list twice (once to see if the value can be inserted and once to do
         * the actual insert), so we assume this value can be inserted and
         * convert the ziplist to a regular list if necessary. */
        tlistTryConversion(lobj,val);

        if (lobj->encoding == REDIS_ENCODING_ZIPLIST) {
            unsigned char *eptr = ziplistIndex(lobj->ptr,0);
            robj *rawval = getDecodedObject(val);

            while (eptr != NULL) {
                if (ziplistCompare(eptr,refval->ptr,sdslen(refval->ptr))) {
                    if (where == REDIS_TAIL) {
                        /* Insert *after* this element. */
                        eptr = ziplistNext(lobj->ptr,eptr);
                        if (eptr != NULL)
                            lobj->ptr = ziplistInsert(lobj->ptr,eptr,
                                rawval->ptr,sdslen(rawval->ptr));
                        else
                            lobj->ptr = ziplistPush(lobj->ptr,
                                rawval->ptr,sdslen(rawval->ptr),ZIPLIST_TAIL);
                    } else {
                        /* Insert *at* this element. */
                        lobj->ptr = ziplistInsert(lobj->ptr,eptr,
                            rawval->ptr,sdslen(rawval->ptr));
                    }

                    /* Check if the ziplist needs to be converted. */
                    if (ziplistLen(lobj->ptr) > server.list_max_ziplist_entries)
                        tlistConvert(lobj,REDIS_ENCODING_LINKEDLIST);

                    inserted = 1;
                    break;
                }

                /* Move to next element. */
                eptr = ziplistNext(lobj->ptr,eptr);
            }
            decrRefCount(rawval);
        } else if (lobj->encoding == REDIS_ENCODING_LINKEDLIST) {
            list *list = lobj->ptr;
            listNode *ln = listFirst(list);

            while (ln != NULL) {
                if (equalStringObjects(listNodeValue(ln),refval)) {
                    if (where == REDIS_TAIL)
                        listInsertNode(list,ln,val,AL_START_TAIL);
                    else
                        listInsertNode(list,ln,val,AL_START_HEAD);

                    incrRefCount(val);
                    inserted = 1;
                    break;
                }

                /* Move to next element. */
                ln = listNextNode(ln);
            }
        } else {
            redisPanic("Unknown list encoding");
        }

        if (inserted) {
            signalModifiedKey(c->db,key);
            server.dirty++;
        } else {
            /* Notify client of a failed insert */
            addReply(c,shared.cnegone);
            return;
        }
    } else {
        tlistPush(lobj,val,where);
        signalModifiedKey(c->db,key);
        server.dirty++;
    }

    addReplyLongLong(c,tlistLength(lobj));
}

void lpushxCommand(redisClient *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    pushxGenericCommand(c,NULL,c->argv[2],REDIS_HEAD);
}

void rpushxCommand(redisClient *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    pushxGenericCommand(c,NULL,c->argv[2],REDIS_TAIL);
}

void linsertCommand(redisClient *c) {
    c->argv[4] = tryObjectEncoding(c->argv[4]);
    if (strcasecmp(c->argv[2]->ptr,"after") == 0) {
        pushxGenericCommand(c,c->argv[3],c->argv[4],REDIS_TAIL);
    } else if (strcasecmp(c->argv[2]->ptr,"before") == 0) {
        pushxGenericCommand(c,c->argv[3],c->argv[4],REDIS_HEAD);
    } else {
        addReply(c,shared.syntaxerr);
    }
}

void llenCommand(redisClient *c) {
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.czero);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;
    addReplyLongLong(c,tlistLength(o));
}

void lindexCommand(redisClient *c) {
    robj *o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;
    int index = atoi(c->argv[2]->ptr);
    robj *value = NULL;

    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *p;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        p = ziplistIndex(o->ptr,index);
        if (ziplistGet(p,&vstr,&vlen,&vlong)) {
            if (vstr) {
                value = createStringObject((char*)vstr,vlen);
            } else {
                value = createStringObjectFromLongLong(vlong);
            }
            addReplyBulk(c,value);
            decrRefCount(value);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        listNode *ln = listIndex(o->ptr,index);
        if (ln != NULL) {
            value = listNodeValue(ln);
            addReplyBulk(c,value);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else {
        redisPanic("Unknown list encoding");
    }
}

void lsetCommand(redisClient *c) {
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;
    int index = atoi(c->argv[2]->ptr);
    robj *value = (c->argv[3] = tryObjectEncoding(c->argv[3]));

    tlistTryConversion(o,value);
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *p, *zl = o->ptr;
        p = ziplistIndex(zl,index);
        if (p == NULL) {
            addReply(c,shared.outofrangeerr);
        } else {
            o->ptr = ziplistDelete(o->ptr,&p);
            value = getDecodedObject(value);
            o->ptr = ziplistInsert(o->ptr,p,value->ptr,sdslen(value->ptr));
            decrRefCount(value);
            addReply(c,shared.ok);
            signalModifiedKey(c->db,c->argv[1]);
            server.dirty++;
        }
    } else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        listNode *ln = listIndex(o->ptr,index);
        if (ln == NULL) {
            addReply(c,shared.outofrangeerr);
        } else {
            decrRefCount((robj*)listNodeValue(ln));
            listNodeValue(ln) = value;
            incrRefCount(value);
            addReply(c,shared.ok);
            signalModifiedKey(c->db,c->argv[1]);
            server.dirty++;
        }
    } else {
        redisPanic("Unknown list encoding");
    }
}

void popGenericCommand(redisClient *c, int where) {
    robj *o = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk);
    if (o == NULL || checkType(c,o,REDIS_LIST)) return;

    robj *value = tlistPop(o,where);
    if (value == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        addReplyBulk(c,value);
        decrRefCount(value);
        if (tlistLength(o) == 0) dbDelete(c->db,c->argv[1]);
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }
}

void lpopCommand(redisClient *c) {
    popGenericCommand(c,REDIS_HEAD);
}

void rpopCommand(redisClient *c) {
    popGenericCommand(c,REDIS_TAIL);
}

void lrangeCommand(redisClient *c) {
    robj *o;
    int start = atoi(c->argv[2]->ptr);
    int end = atoi(c->argv[3]->ptr);
    int llen;
    int rangelen;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
         || checkType(c,o,REDIS_LIST)) return;
    llen = tlistLength(o);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c,rangelen);
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *p = ziplistIndex(o->ptr,start);
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        while(rangelen--) {
            ziplistGet(p,&vstr,&vlen,&vlong);
            if (vstr) {
                addReplyBulkCBuffer(c,vstr,vlen);
            } else {
                addReplyBulkLongLong(c,vlong);
            }
            p = ziplistNext(o->ptr,p);
        }
    } else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        listNode *ln = listIndex(o->ptr,start);

        while(rangelen--) {
            addReplyBulk(c,ln->value);
            ln = ln->next;
        }
    } else {
        redisPanic("List encoding is not LINKEDLIST nor ZIPLIST!");
    }
}

void ltrimCommand(redisClient *c) {
    robj *o;
    int start = atoi(c->argv[2]->ptr);
    int end = atoi(c->argv[3]->ptr);
    int llen;
    int j, ltrim, rtrim;
    list *list;
    listNode *ln;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.ok)) == NULL ||
        checkType(c,o,REDIS_LIST)) return;
    llen = tlistLength(o);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* Remove list elements to perform the trim */
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        o->ptr = ziplistDeleteRange(o->ptr,0,ltrim);
        o->ptr = ziplistDeleteRange(o->ptr,-rtrim,rtrim);
    } else if (o->encoding == REDIS_ENCODING_LINKEDLIST) {
        list = o->ptr;
        for (j = 0; j < ltrim; j++) {
            ln = listFirst(list);
            listDelNode(list,ln);
        }
        for (j = 0; j < rtrim; j++) {
            ln = listLast(list);
            listDelNode(list,ln);
        }
    } else {
        redisPanic("Unknown list encoding");
    }
    if (tlistLength(o) == 0) dbDelete(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
    addReply(c,shared.ok);
}

void lremCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *lobj, *ele;
    int toremove = atoi(c->argv[2]->ptr);
    int removed = 0;
    int reverse = 0;

    if ((lobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
            checkType(c,lobj,REDIS_LIST)) return;

    if (toremove < 0) {
        toremove = -toremove;
        reverse = 1;
    }

    if (lobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *eptr;
        ele = c->argv[3];

        if (reverse)
            eptr = ziplistIndex(lobj->ptr,-1);
        else
            eptr = ziplistIndex(lobj->ptr,0);

        while (eptr != NULL && (toremove == 0 || removed < toremove)) {
            if (ziplistCompare(eptr,ele->ptr,sdslen(ele->ptr))) {
                lobj->ptr = ziplistDelete(lobj->ptr,&eptr);
                removed++;
                server.dirty++;

                /* When traversing from tail to head, eptr will now point to an
                 * element we already checked. So, we can safely move back. */
                if (reverse && eptr)
                    eptr = ziplistPrev(lobj->ptr,eptr);

                /* eptr now points to an unchecked element. */
                continue;
            }

            if (reverse)
                eptr = ziplistPrev(lobj->ptr,eptr);
            else
                eptr = ziplistNext(lobj->ptr,eptr);
        }
    } else if (lobj->encoding == REDIS_ENCODING_LINKEDLIST) {
        list *l = lobj->ptr;
        listNode *ln, *aux;
        ele = c->argv[3] = tryObjectEncoding(c->argv[3]);

        if (reverse)
            ln = listLast(l);
        else
            ln = listFirst(l);

        while (ln != NULL && (toremove == 0 || removed < toremove)) {
            if (reverse)
                aux = listPrevNode(ln);
            else
                aux = listNextNode(ln);

            if (equalStringObjects(listNodeValue(ln),ele)) {
                listDelNode(l,ln);
                removed++;
                server.dirty++;
            }

            ln = aux;
        }
    } else {
        redisPanic("Unknown list encoding");
    }

    if (tlistLength(lobj) == 0) dbDelete(c->db,key);
    addReplyLongLong(c,removed);
    if (removed) signalModifiedKey(c->db,key);
}

/* This is the semantic of this command:
 *  RPOPLPUSH srclist dstlist:
 *    IF LLEN(srclist) > 0
 *      element = RPOP srclist
 *      LPUSH dstlist element
 *      RETURN element
 *    ELSE
 *      RETURN nil
 *    END
 *  END
 *
 * The idea is to be able to get an element from a list in a reliable way
 * since the element is not just returned but pushed against another list
 * as well. This command was originally proposed by Ezra Zygmuntowicz.
 */

void rpoplpushHandlePush(redisClient *c, robj *dstkey, robj *dstobj, robj *value) {
    if (!handleClientsWaitingListPush(c,dstkey,value)) {
        /* Create the list if the key does not exist */
        if (!dstobj) {
            dstobj = createZiplistObject();
            dbAdd(c->db,dstkey,dstobj);
        } else {
            signalModifiedKey(c->db,dstkey);
            server.dirty++;
        }
        tlistPush(dstobj,value,REDIS_HEAD);
    }

    /* Always send the pushed value to the client. */
    addReplyBulk(c,value);
}

void rpoplpushCommand(redisClient *c) {
    robj *sobj, *value;
    if ((sobj = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,sobj,REDIS_LIST)) return;

    if (tlistLength(sobj) == 0) {
        addReply(c,shared.nullbulk);
    } else {
        robj *dobj = lookupKeyWrite(c->db,c->argv[2]);
        if (dobj && checkType(c,dobj,REDIS_LIST)) return;
        value = tlistPop(sobj,REDIS_TAIL);
        rpoplpushHandlePush(c,c->argv[2],dobj,value);

        /* tlistPop returns an object with its refcount incremented */
        decrRefCount(value);

        /* Delete the source list when it is empty */
        if (tlistLength(sobj) == 0) dbDelete(c->db,c->argv[1]);
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }
}

/*-----------------------------------------------------------------------------
 * Blocking POP operations
 *----------------------------------------------------------------------------*/

/* Currently Redis blocking operations support is limited to list POP ops,
 * so the current implementation is not fully generic, but it is also not
 * completely specific so it will not require a rewrite to support new
 * kind of blocking operations in the future.
 *
 * Still it's important to note that list blocking operations can be already
 * used as a notification mechanism in order to implement other blocking
 * operations at application level, so there must be a very strong evidence
 * of usefulness and generality before new blocking operations are implemented.
 *
 * This is how the current blocking POP works, we use BLPOP as example:
 * - If the user calls BLPOP and the key exists and contains a non empty list
 *   then LPOP is called instead. So BLPOP is semantically the same as LPOP
 *   if there is not to block.
 * - If instead BLPOP is called and the key does not exists or the list is
 *   empty we need to block. In order to do so we remove the notification for
 *   new data to read in the client socket (so that we'll not serve new
 *   requests if the blocking request is not served). Also we put the client
 *   in a dictionary (db->blocking_keys) mapping keys to a list of clients
 *   blocking for this keys.
 * - If a PUSH operation against a key with blocked clients waiting is
 *   performed, we serve the first in the list: basically instead to push
 *   the new element inside the list we return it to the (first / oldest)
 *   blocking client, unblock the client, and remove it form the list.
 *
 * The above comment and the source code should be enough in order to understand
 * the implementation and modify / fix it later.
 */

/* Set a client in blocking mode for the specified key, with the specified
 * timeout */
void blockForKeys(redisClient *c, robj **keys, int numkeys, time_t timeout, robj *target) {
    dictEntry *de;
    list *l;
    int j;

    c->bpop.keys = zmalloc(sizeof(robj*)*numkeys);
    c->bpop.count = numkeys;
    c->bpop.timeout = timeout;
    c->bpop.target = target;

    if (target != NULL) {
        incrRefCount(target);
    }

    for (j = 0; j < numkeys; j++) {
        /* Add the key in the client structure, to map clients -> keys */
        c->bpop.keys[j] = keys[j];
        incrRefCount(keys[j]);

        /* And in the other "side", to map keys -> clients */
        de = dictFind(c->db->blocking_keys,keys[j]);
        if (de == NULL) {
            int retval;

            /* For every key we take a list of clients blocked for it */
            l = listCreate();
            retval = dictAdd(c->db->blocking_keys,keys[j],l);
            incrRefCount(keys[j]);
            redisAssert(retval == DICT_OK);
        } else {
            l = dictGetEntryVal(de);
        }
        listAddNodeTail(l,c);
    }
    /* Mark the client as a blocked client */
    c->flags |= REDIS_BLOCKED;
    server.bpop_blocked_clients++;
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP */
void unblockClientWaitingData(redisClient *c) {
    dictEntry *de;
    list *l;
    int j;

    redisAssert(c->bpop.keys != NULL);
    /* The client may wait for multiple keys, so unblock it for every key. */
    for (j = 0; j < c->bpop.count; j++) {
        /* Remove this client from the list of clients waiting for this key. */
        de = dictFind(c->db->blocking_keys,c->bpop.keys[j]);
        redisAssert(de != NULL);
        l = dictGetEntryVal(de);
        listDelNode(l,listSearchKey(l,c));
        /* If the list is empty we need to remove it to avoid wasting memory */
        if (listLength(l) == 0)
            dictDelete(c->db->blocking_keys,c->bpop.keys[j]);
        decrRefCount(c->bpop.keys[j]);
    }

    /* Cleanup the client structure */
    zfree(c->bpop.keys);
    c->bpop.keys = NULL;
    c->bpop.target = NULL;
    c->flags &= ~REDIS_BLOCKED;
    c->flags |= REDIS_UNBLOCKED;
    server.bpop_blocked_clients--;
    listAddNodeTail(server.unblocked_clients,c);
}

/* This should be called from any function PUSHing into lists.
 * 'c' is the "pushing client", 'key' is the key it is pushing data against,
 * 'ele' is the element pushed.
 *
 * If the function returns 0 there was no client waiting for a list push
 * against this key.
 *
 * If the function returns 1 there was a client waiting for a list push
 * against this key, the element was passed to this client thus it's not
 * needed to actually add it to the list and the caller should return asap. */
int handleClientsWaitingListPush(redisClient *c, robj *key, robj *ele) {
    struct dictEntry *de;
    redisClient *receiver;
    int numclients;
    list *clients;
    listNode *ln;
    robj *dstkey, *dstobj;

    de = dictFind(c->db->blocking_keys,key);
    if (de == NULL) return 0;
    clients = dictGetEntryVal(de);
    numclients = listLength(clients);

    /* Try to handle the push as long as there are clients waiting for a push.
     * Note that "numclients" is used because the list of clients waiting for a
     * push on "key" is deleted by unblockClient() when empty.
     *
     * This loop will have more than 1 iteration when there is a BRPOPLPUSH
     * that cannot push the target list because it does not contain a list. If
     * this happens, it simply tries the next client waiting for a push. */
    while (numclients--) {
        ln = listFirst(clients);
        redisAssert(ln != NULL);
        receiver = ln->value;
        dstkey = receiver->bpop.target;

        /* This should remove the first element of the "clients" list. */
        unblockClientWaitingData(receiver);
        redisAssert(ln != listFirst(clients));

        if (dstkey == NULL) {
            /* BRPOP/BLPOP */
            addReplyMultiBulkLen(receiver,2);
            addReplyBulk(receiver,key);
            addReplyBulk(receiver,ele);
            return 1;
        } else {
            /* BRPOPLPUSH, note that receiver->db is always equal to c->db. */
            dstobj = lookupKeyWrite(receiver->db,dstkey);
            if (dstobj && checkType(receiver,dstobj,REDIS_LIST)) {
                decrRefCount(dstkey);
            } else {
                rpoplpushHandlePush(receiver,dstkey,dstobj,ele);
                decrRefCount(dstkey);
                return 1;
            }
        }
    }

    return 0;
}

int getTimeoutFromObjectOrReply(redisClient *c, robj *object, time_t *timeout) {
    long tval;

    if (getLongFromObjectOrReply(c,object,&tval,
        "timeout is not an integer or out of range") != REDIS_OK)
        return REDIS_ERR;

    if (tval < 0) {
        addReplyError(c,"timeout is negative");
        return REDIS_ERR;
    }

    if (tval > 0) tval += time(NULL);
    *timeout = tval;

    return REDIS_OK;
}

/* Blocking RPOP/LPOP */
void blockingPopGenericCommand(redisClient *c, int where) {
    robj *o;
    time_t timeout;
    int j;

    if (getTimeoutFromObjectOrReply(c,c->argv[c->argc-1],&timeout) != REDIS_OK)
        return;

    for (j = 1; j < c->argc-1; j++) {
        o = lookupKeyWrite(c->db,c->argv[j]);
        if (o != NULL) {
            if (o->type != REDIS_LIST) {
                addReply(c,shared.wrongtypeerr);
                return;
            } else {
                if (tlistLength(o) != 0) {
                    /* If the list contains elements fall back to the usual
                     * non-blocking POP operation */
                    robj *argv[2], **orig_argv;
                    int orig_argc;

                    /* We need to alter the command arguments before to call
                     * popGenericCommand() as the command takes a single key. */
                    orig_argv = c->argv;
                    orig_argc = c->argc;
                    argv[1] = c->argv[j];
                    c->argv = argv;
                    c->argc = 2;

                    /* Also the return value is different, we need to output
                     * the multi bulk reply header and the key name. The
                     * "real" command will add the last element (the value)
                     * for us. If this souds like an hack to you it's just
                     * because it is... */
                    addReplyMultiBulkLen(c,2);
                    addReplyBulk(c,argv[1]);

                    popGenericCommand(c,where);

                    /* Fix the client structure with the original stuff */
                    c->argv = orig_argv;
                    c->argc = orig_argc;

                    return;
                }
            }
        }
    }

    /* If we are inside a MULTI/EXEC and the list is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    if (c->flags & REDIS_MULTI) {
        addReply(c,shared.nullmultibulk);
        return;
    }

    /* If the list is empty or the key does not exists we must block */
    blockForKeys(c, c->argv + 1, c->argc - 2, timeout, NULL);
}

void blpopCommand(redisClient *c) {
    blockingPopGenericCommand(c,REDIS_HEAD);
}

void brpopCommand(redisClient *c) {
    blockingPopGenericCommand(c,REDIS_TAIL);
}

void brpoplpushCommand(redisClient *c) {
    time_t timeout;

    if (getTimeoutFromObjectOrReply(c,c->argv[3],&timeout) != REDIS_OK)
        return;

    robj *key = lookupKeyWrite(c->db, c->argv[1]);

    if (key == NULL) {
        if (c->flags & REDIS_MULTI) {

            /* Blocking against an empty list in a multi state
             * returns immediately. */
            addReply(c, shared.nullbulk);
        } else {
            /* The list is empty and the client blocks. */
            blockForKeys(c, c->argv + 1, 1, timeout, c->argv[2]);
        }
    } else {
        if (key->type != REDIS_LIST) {
            addReply(c, shared.wrongtypeerr);
        } else {

            /* The list exists and has elements, so
             * the regular rpoplpushCommand is executed. */
            redisAssert(tlistLength(key) > 0);
            rpoplpushCommand(c);
        }
    }
}
