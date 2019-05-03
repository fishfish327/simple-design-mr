#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include "mapreduce.h"
#include "data_structure.h"


#define Pthread_create(thread,attr,start_routine,arg) assert(pthread_create(thread,attr,start_routine,arg)==0);
#define Pthread_join(thread,value_ptr) assert(pthread_join(thread,value_ptr)==0);
#define Pthread_mutex_lock(m) assert(pthread_mutex_lock(m)==0);
#define Pthread_mutex_unlock(m) assert(pthread_mutex_unlock(m)==0);

#define true 1
#define false 0





//variables for grabbing files
char** files;
pthread_mutex_t filetableLock;
int numFiles;
int numOfFilesLeft;

//Variables for partition
Partition **partitionArray;
int numOfPartition;
//Variables for mappers and reducers
Mapper _Map;
Reducer _Reduce;
Partitioner _Partition;

char* get_next(char* key, int partition_number)
{   // find the conresponding partition, kvSet, check key with the KVSet
	Partition* currPartition = partitionArray[partition_number];
	//pthread_mutex_lock(&(currPartition->partitionLock));
	if(key == NULL){
		printf("key error\n");
	}
	
    KVListNode *curr = currPartition->listHead;
    // read value from values array
    if(curr == NULL){
		return NULL;
	}
    if(strcmp(key, curr->key) != 0)
    {
        return NULL;
    }
	//pthread_mutex_unlock(&(currPartition->partitionLock));
    char *value = curr->value;
	//pthread_mutex_lock(&(currPartition->partitionLock));
	currPartition->listHead = curr->next;
	curr->next = NULL;
	addKVListNode(currPartition, curr, 0);
	//pthread_mutex_unlock(&(currPartition->partitionLock));
    return value;
}


// rewrite this part !!!!
void* GrabFileAndMap()
{
	int i; char* filename;

	grabNewFile:

		filename=NULL;

		//Note: mapper threads should repetitively come here until there's no file left to map.
		//We first grab filetable lock. Check if there's still any file to Map().
		//if there is, grab the file from files[i], set files[i]=NULL, map() the file
		//else, just exit the thread

		Pthread_mutex_lock(&filetableLock);


		//if numOfFiles left is zero, no need to go into loop anymore
		if(numOfFilesLeft==0)
		{

			Pthread_mutex_unlock(&filetableLock);
			pthread_exit(0);
		}

		for(i=0;i<numFiles;i++)
		{
			if(files[i]!=NULL)
			{
				filename=strdup(files[i]);
				free(files[i]);
				files[i]=NULL;
				numOfFilesLeft--;
				break;
			}

		}


		Pthread_mutex_unlock(&filetableLock);


		//if the mapper thread is unable to grab a file, exit it
		assert(filename!=NULL);
		// if(filename==NULL)
		// {
		// 	printf("Something funny happened here.\n");
		// }

		//////////////////////////////////



	_Map(filename);
	
	free(filename);

	if(numOfFilesLeft==0)
		pthread_exit(0);
	else
		goto grabNewFile;


}

//  Store <key,value> into partition

void MR_Emit(char *key, char *value)
{
	unsigned long partitionNumber=_Partition(key, numOfPartition);
	
	Partition *targetPartition=partitionArray[partitionNumber];
	pthread_mutex_lock(&(targetPartition->partitionLock));
	// this check can be deleted
	if(strlen(key) == 0){
	   pthread_mutex_unlock(&(targetPartition->partitionLock));
       return;
	}

	KVListNode* tmp = malloc(sizeof(KVListNode));
	initKVListNode(tmp, key, value);
	// 1 for flag for listhead
	addKVListNode(targetPartition, tmp, 1);
	pthread_mutex_unlock(&(targetPartition->partitionLock));
		
	return;

}

void* SortPartitionsAndReduce(void* numPartition)
{
	int pIndex = *(int *)numPartition;
	
	Partition * target = partitionArray[pIndex];
	//pthread_mutex_lock(&(target->partitionLock));
    target->listHead = sortListNode(target->listHead);
	target->numOfK = countDistinctKey(target->listHead);
	
	//pthread_mutex_unlock(&(target->partitionLock));
	int num = target->numOfK;
    target-> keyArray = collectKey(target->listHead, num);
	for(int i =0; i < num; i++){
		
		char* currKey = target->keyArray[i];
		_Reduce(currKey, get_next, pIndex);
	}
	//pthread_mutex_lock(&(target->partitionLock));
	//freePartitionData(target);
	//pthread_mutex_unlock(&(target->partitionLock));
	pthread_exit(0);
}


// rewrite this part
void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition)
{
	int i;
	pthread_t mappers[num_mappers];
	pthread_t reducers[num_reducers];
	int** partitionNumber=NULL;

	if(argc==1)
	{
		printf("Bad argument. Usage: ./mapreduce <filename_0> <filename_1> <filename_2> ... \n");
		exit(1);
	}
	numFiles=argc-1;
	numOfFilesLeft=numFiles;
	numOfPartition=num_reducers;
	_Map=map;
	_Reduce=reduce;
	_Partition=partition;

	//initialize lock for filetable
	pthread_mutex_init(&filetableLock,NULL);


	partitionNumber=malloc(num_reducers*sizeof(int*));

	//create partition arrays
	/**
	*   Design
	*   _________________________________________________________________________
	*   |           |           |           |           |           |           |
	*   | [  |  |  ]| [  |  |  ]| [  |  |  ]| [  |  |  ]| [  |  |  ]| [  |  |  ]|
	*   |           |           |           |           |           |           |
	*   |           |           |           |           |           |           |
	*   |___________|___________|___________|___________|___________|___________|
	*        P1           P2         P3          P4           P5          P6
	*/ 

	partitionArray=malloc(num_reducers*sizeof(Partition*));

	//initialize each partition in the partition array
	for(i=0;i<num_reducers;i++)
	{
		partitionArray[i]=malloc(sizeof(Partition));
		Partition *currentPartition=partitionArray[i];
		currentPartition->listHead=NULL;
		currentPartition->numOfK=0;
		currentPartition->keyArray = NULL;
		
		pthread_mutex_init(&(currentPartition->partitionLock),NULL);
		
			
		
	}																					  


	
	//copy argv arrays to files array so that I can modify the content in files array
	files=malloc((numFiles)*sizeof(char*));
	for(i=0;i<numFiles;i++)
	{
		files[i]=strdup(argv[i+1]);
		//strcpy(files[i],argv[i+1]);
	}


	for(i=0;i<num_mappers;i++)
	{
		Pthread_create(&mappers[i],NULL,GrabFileAndMap,NULL);
	}


	for(i=0;i<num_mappers;i++)
	{
		Pthread_join(mappers[i],NULL);
	}

	// At this point here in method, mappers have finish doing all the work
	free(files);


	//sort the keys in the partition and reduce
	for(i=0;i<num_reducers;i++)
	{
		partitionNumber[i]=malloc(sizeof(int));
		*(partitionNumber[i])=i;
		Pthread_create(&reducers[i],NULL,SortPartitionsAndReduce,(void*)partitionNumber[i]);
	}

	for(i=0;i<num_reducers;i++)
	{
		Pthread_join(reducers[i],NULL);
	}


	// At this point here in method, reducers have finish doing all the work
	
	for(i=0;i<num_reducers;i++)
	{
		freePartitionData(partitionArray[i]);
		pthread_mutex_destroy(&(partitionArray[i]->partitionLock));
		free(partitionArray[i]);
		free(partitionNumber[i]);
	}
	free(partitionArray);
	free(partitionNumber);



	pthread_mutex_destroy(&filetableLock);





}




//////////////////////////////////////The functions below are customizable to suit for the job in hand

//A simple application of Map Reduce in word counting for arbitrary number of files
//Feel free to change anything you like in the functions below to suit the job in hand

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
	unsigned long hash=5381;
	int c;
	while((c=*key++)!='\0')
		hash=hash*33 +c;
	return (hash%num_partitions);
}


void Map(char* file_name)
{
	FILE *fp=fopen(file_name,"r");
	assert(fp!=NULL);

	char* line=NULL;
	size_t size=0;
	while(getline(&line,&size,fp)!=-1)
	{
		//token, dummy equals to a line in the file
		char *token, *dummy=line;
		//while curr pointer doesn't reach the end of current line
		while((token=strsep(&dummy," \t\n\r"))!=NULL)
		{
			MR_Emit(token,"1");
		}
	}
	free(line);
	fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number)
{
	int count=0;
	char* value;
	//iterate all the values that produced the same key
	while((value=get_next(key,partition_number))!=NULL)
		count++;
	printf("%s %d\n",key,count);
}

int main(int argc, char* argv[])
{
					//Num of mapper thread
					//		|
					//		|	Number of reducer thread
					//		|		   |
					//		↓		   ↓
	MR_Run(argc, argv, Map,2 , Reduce, 10, MR_DefaultHashPartition);
	return 0;
}

