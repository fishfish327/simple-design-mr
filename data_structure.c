#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "data_structure.h"
#define SIZE_VALUESARRAY (5000)
void printKVList(KVListNode * head);

void initKVListNode(struct KVListNode* node, char* key, char* value){
     node->key = strdup(key);
     node->value = strdup(value);
     node->next = NULL;
}
void addKVListNode(struct Partition* p,struct KVListNode* curr, int flag){    
     if(flag == 1){
        if(p->listHead == NULL){
           p->listHead = curr;
        }else {
           curr->next = p->listHead;
	       p->listHead = curr;
        }
     } else if(flag == 0){
        if(p->usedNode == NULL){
           p->usedNode = curr;
        }else {
           curr->next = p->usedNode;
	       p->usedNode = curr;
        }
     }
     
}
int countDistinctKey(KVListNode * sortedList){
    int res = 0;
    KVListNode * prev = NULL;
    KVListNode * curr = sortedList;
    while(curr != NULL){
        if(prev == NULL || strcmp(prev->key, curr->key) != 0){
            res ++;
            prev = curr;
        }
        curr = curr->next;
    }
    return res;
}
KVListNode* mergeTwoLists(KVListNode* l1, KVListNode* l2){
    if(l1 == NULL) return l2;
    if(l2 == NULL) return l1;
    if(l1->key == NULL || l2->key == NULL){
        printf("catch error\n");
        exit(-1);
    }
    if(strcmp(l1->key, l2->key) < 0){
       l1->next = mergeTwoLists(l1->next, l2);
       return l1;
    } else {
        l2->next = mergeTwoLists(l1, l2->next);
        return l2;
    }
}
// new method for 
KVListNode* sortListNode(KVListNode* head){
    }



// datastruce KVSet, collect values with same key together

void printKVList(KVListNode * head){
    KVListNode *curr = head;
    while(curr != NULL){
        if(curr->key == NULL || curr->value==0)
        {
            printf("hit error\n");
            return;
        }
        printf("%s -> %s\n", curr->key, curr->value);
        curr = curr -> next;
    }
    printf("------------------------------\n");
}

void freeKVList(KVListNode * head){
    KVListNode* curr = head;
    while(curr != NULL){
        KVListNode *tmp = curr;
        curr = curr-> next;
        free(tmp);
    }
}
char** collectKey(KVListNode * target, int len){
    char **keyArray;
    if(len <= 0){
        return NULL;
    }
    else {
       keyArray = malloc(len * sizeof(char*));
       KVListNode * curr = target;
       int i = 0;
       char * prev;
       char * currChar; 
       while(curr != NULL && i < len){
           currChar = curr -> key;
           if(prev == NULL){
               prev = currChar;
               keyArray[i] = strdup(currChar);
               i++;
           } else if(strcmp(currChar, prev) != 0){
               keyArray[i] = strdup(currChar);
               i++;
               prev = currChar;
           }
           curr = curr->next;
       }
       assert(i == len);
    }
   // for(int j =0; j < len; j++){
     //   printf("%s\n", keyArray[j]);
   // }
    return keyArray;
    
}
void freePartitionData(Partition * target){
    int num = target->numOfK;
    freeKVList(target->usedNode);
    for(int i =0; i < num; i++){
        free(target->keyArray[i]);
    }
    free(target->keyArray);
}
/*int main(){
    Partition * p = malloc(sizeof(Partition));
    KVListNode *l1 = malloc(sizeof(KVListNode));
    initKVListNode(l1, "one", "1");
    KVListNode *l2 = malloc(sizeof(KVListNode));
    initKVListNode(l2, "one", "1");
    KVListNode *l3 = malloc(sizeof(KVListNode));
    initKVListNode(l3, "two", "1");
    KVListNode *l4 = malloc(sizeof(KVListNode));
    initKVListNode(l4, "three", "1");
    KVListNode *l5 = malloc(sizeof(KVListNode));
    initKVListNode(l5, "two", "1");
    l1->next = l2;
    l2->next = l3;
    l3->next = l4;
    l4->next = l5;
    p->listHead = l1;
    sortListNode(p->listHead);
    p->numOfK = countDistinctKey(p->listHead);
    assert(p->numOfK == 3);
    collectKey(p);
    char * a = p->keyArray[0];
    char * b = p->keyArray[1];
    char * c = p->keyArray[2];
    printf("%s%s%s\n", a, b,c);
   // assert(strcmp(p->keyArray[0], "one") == 0); 
    //printf("key is: %s", p->keyArray[1]);
    //assert(strcmp(p->keyArray[1], "two") == 0);
   // assert(strcmp(p->keyArray[2], "three") == 0);
    freePartitionData(p);
    free(p);
}*/


