#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <mpi.h>

void Merge(char* initialArr,char* Merged,unsigned long start,unsigned long end,unsigned long size);
void MergePass(char* iniArr,char* targetArr,unsigned long AllArrSize,unsigned long mergeSize);
void MergeSort(char* target,unsigned long size);

//Totalsize should be 1000000000 (10^9)
int main(int argc,char* argv[])
{
	int myrank,mysize;
	char *MainData,*RecvData;
	unsigned long  TotalSize,LocalSize;
	double StartTime,EndTime,TimeTag,ModifyTag;

	srand(time(NULL));

	MPI_Status Status;
	MPI_Init(&argc,&argv);
	MPI_Comm_size(MPI_COMM_WORLD,&mysize);
	MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
	
	TotalSize = 1000000000;
	LocalSize = (unsigned long)(TotalSize/mysize);
	
	MainData = calloc(TotalSize,sizeof(char));
	RecvData = calloc(TotalSize,sizeof(char));
	
	if(myrank == 0)
	{
		for(unsigned long i=0;i<TotalSize;i++)
		{
			MainData[i] = rand()%101;
		}
	}
	MPI_Barrier(MPI_COMM_WORLD);
	
	StartTime = clock();
	
	MPI_Scatter(MainData,LocalSize,MPI_CHAR,MainData,LocalSize,MPI_CHAR,0,MPI_COMM_WORLD);
	MergeSort(MainData,LocalSize);
	MPI_Gather(MainData,LocalSize,MPI_CHAR,MainData,LocalSize,MPI_CHAR,0,MPI_COMM_WORLD);
	
	TimeTag = clock();
	
	MPI_Bcast(MainData,TotalSize,MPI_CHAR,0,MPI_COMM_WORLD);
	
	unsigned long ptr[mysize];
	unsigned long ptrBoundary[mysize];
	
	for(int i=0;i<mysize;i++)
	{
		ptr[i] = i*LocalSize+(myrank*LocalSize/mysize);
		ptrBoundary[i] = i*LocalSize+((myrank+1)*LocalSize/mysize);
	}
	
	for(unsigned long i=0;i<LocalSize;)
	{
		int NextPtrIndex = 0;
		for(int j=0;j<mysize;j++)
		{
			if(ptr[j]<ptrBoundary[j])
			{
				if(MainData[ptr[j]] <= MainData[ptr[NextPtrIndex]])
				{
					NextPtrIndex = j;
				}
			}
		}
		RecvData[i] = MainData[ptr[NextPtrIndex]];
		ptr[NextPtrIndex]++;
		i++;
	}
	
	MPI_Gather(RecvData,LocalSize,MPI_CHAR,RecvData,LocalSize,MPI_CHAR,0,MPI_COMM_WORLD);
	
	//EndTime = clock();
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	ModifyTag = clock();
	
	if(myrank == 0)
	{
		for(unsigned long i=0;i<TotalSize-1;i++)
		{
			if(RecvData[i]>RecvData[i+1])
			{
				unsigned long CorrectSmallStart,CorrectBigEnd;
				for(CorrectSmallStart = i;RecvData[CorrectSmallStart]!=RecvData[i+1];CorrectSmallStart--);
				CorrectSmallStart++;
				for(CorrectBigEnd = i+1;RecvData[CorrectBigEnd]!=RecvData[i];CorrectBigEnd++);
				CorrectBigEnd--;
				char tmp;
				while(RecvData[CorrectSmallStart]>RecvData[CorrectBigEnd])
				{
					tmp = RecvData[CorrectSmallStart];
					RecvData[CorrectSmallStart] = RecvData[CorrectBigEnd];
					RecvData[CorrectBigEnd] = tmp;
					CorrectSmallStart++;
					CorrectBigEnd--;
				}
			}
		}
	}
	
	EndTime = clock();
	
	if(myrank == 0)
	{
		for(unsigned long i=0;i<TotalSize-1;i++)
		{
			if(RecvData[i]>RecvData[i+1])
			{
				printf("Error occur at %lu with data %d,%d\n",i,RecvData[i],RecvData[i+1]);
			}
		}
	}
	
	if(myrank == 0)
	{
		printf("Time Cost is %f\n",(double)(EndTime-StartTime)/(CLOCKS_PER_SEC));
		printf("Time Cost for MS %f\n",(double)(TimeTag-StartTime)/(CLOCKS_PER_SEC));
		printf("Time Cost for without modify %f\n",(double)(ModifyTag-StartTime)/(CLOCKS_PER_SEC));
	}
	
	MPI_Finalize();
	return 0;
}

void Merge(char* initialArr,char* Merged,unsigned long start,unsigned long end,unsigned long size)
{
	unsigned long ptr1 = start,ptr2 = start+size;
	for(unsigned long i=start;i<end+1;i++)
	{
		if(ptr1 < start+size && ptr2 < end+1)
		{
			if(initialArr[ptr1] < initialArr[ptr2])
			{
				Merged[i] = initialArr[ptr1];
				ptr1++;
			}
			else if(initialArr[ptr1] >= initialArr[ptr2])
			{
				Merged[i] = initialArr[ptr2];
				ptr2++;
			}
		}
		else if(ptr2 >= end+1)
		{
			Merged[i] = initialArr[ptr1];
			ptr1++;
		}
		else if(ptr1 >= start+size)
		{
			Merged[i] = initialArr[ptr2];
			ptr2++;
		}
	}
}

void MergePass(char* iniArr,char* targetArr,unsigned long AllArrSize,unsigned long mergeSize)
{
	for(unsigned long i=0;i<AllArrSize;i+=2*mergeSize)
	{
		if(i+2*mergeSize-1 < AllArrSize)
		{
			Merge(iniArr,targetArr,i,i+2*mergeSize-1,mergeSize);
		}
		else
		{
			Merge(iniArr,targetArr,i,AllArrSize-1,mergeSize);
		}
	}
}

void MergeSort(char* target,unsigned long size)
{
	char *tmp = calloc(size,sizeof(char));
	for(unsigned long i=1;i<size;i*=2)
	{
		MergePass(target,tmp,size,i);
		i*=2;
		MergePass(tmp,target,size,i);
	}
}

