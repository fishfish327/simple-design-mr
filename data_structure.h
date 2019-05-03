#include <pthread.h>
struct KVListNode {
	char* key;
	char* value;
	struct KVListNode *next;
};
struct Partition{
	struct KVListNode* listHead;
	struct KVListNode* usedNode;
	pthread_mutex_t partitionLock;
	char **keyArray;
	int numOfK;
};
typedef struct KVListNode KVListNode;
typedef struct Partition Partition;

extern void printKVList(KVListNode * head);
extern void initKVListNode(struct KVListNode* node, char* key, char* value);
extern void addKVListNode(struct Partition* p,struct KVListNode* curr, int flag);
extern KVListNode* sortListNode(KVListNode* head);
int countDistinctKey(KVListNode * sortedList);
extern char **collectKey(KVListNode * target, int len);

extern void freePartitionData(Partition * target);