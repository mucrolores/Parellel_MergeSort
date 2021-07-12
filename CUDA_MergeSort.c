#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <cuda.h>

struct MessageBlock{
	unsigned long TotalSize;
	unsigned long LocalSize;
	unsigned long TotalThreads;
};

__global__ void Message(MessageBlock MB,char MainData[],char TmpData[])
{
	int b = blockIdx.x;
	int t = threadIdx.x;
	int n = blockDim.x;
	int x = b*n+t;
	
	unsigned long Row1Index,Row1End;
	unsigned long Row2Index,Row2End;
	unsigned long LocalStart = x*MB.LocalSize;
	unsigned long LocalEnd = (x+1)*MB.LocalSize;
	
	//printf("x:%d,LoclaStart:%lu,LocalEnd:%lu\n",x,LocalStart,LocalEnd);
	
	for(unsigned long MergeSize=1;MergeSize<MB.LocalSize;MergeSize*=2)
	{
		//printf("%lu\n",MergeSize);
		for(unsigned long LocalMergeStartIndex=LocalStart;LocalMergeStartIndex<LocalEnd;LocalMergeStartIndex+=2*MergeSize)
		{
			if(LocalMergeStartIndex+2*MergeSize < LocalEnd)
			{
				Row1Index = LocalMergeStartIndex;
				Row1End = LocalMergeStartIndex+MergeSize;
				Row2Index = LocalMergeStartIndex+MergeSize;
				Row2End = LocalMergeStartIndex+2*MergeSize;
				for(unsigned long Index=LocalMergeStartIndex;Index<LocalMergeStartIndex+2*MergeSize;Index++)
				{
					if(Row1Index < Row1End && Row2Index < Row2End)
					{
						if(MainData[Row1Index] < MainData[Row2Index])
						{
							TmpData[Index] = MainData[Row1Index];
							Row1Index++;
						}
						else
						{
							TmpData[Index] = MainData[Row2Index];
							Row2Index++;
						}
					}
					else if(Row2Index >= Row2End)
					{
						TmpData[Index] = MainData[Row1Index];
						Row1Index++;
					}
					else if(Row1Index >= Row1End)
					{
						TmpData[Index] = MainData[Row2Index];
						Row2Index++;
					}
				}
			}
			else
			{
				Row1Index = LocalMergeStartIndex;
				Row1End = LocalMergeStartIndex+MergeSize;
				Row2Index = LocalMergeStartIndex+MergeSize;
				Row2End = LocalEnd;
				for(unsigned long Index=LocalMergeStartIndex;Index<LocalEnd;Index++)
				{
					if(Row1Index < Row1End && Row2Index < Row2End)
					{
						if(MainData[Row1Index] < MainData[Row2Index])
						{
							TmpData[Index] = MainData[Row1Index];
							Row1Index++;
						}
						else
						{
							TmpData[Index] = MainData[Row2Index];
							Row2Index++;
						}
					}
					else if(Row2Index >= Row2End)
					{
						TmpData[Index] = MainData[Row1Index];
						Row1Index++;
					}
					else if(Row1Index >= Row1End)
					{
						TmpData[Index] = MainData[Row2Index];
						Row2Index++;
					}
				}
			}	
		}
		
		MergeSize*=2;
		for(unsigned long LocalMergeStartIndex=LocalStart;LocalMergeStartIndex<LocalEnd;LocalMergeStartIndex+=2*MergeSize)
		{
			if(LocalMergeStartIndex+2*MergeSize < LocalEnd)
			{
				Row1Index = LocalMergeStartIndex;
				Row1End = LocalMergeStartIndex+MergeSize;
				Row2Index = LocalMergeStartIndex+MergeSize;
				Row2End = LocalMergeStartIndex+2*MergeSize;
				for(unsigned long Index=LocalMergeStartIndex;Index<LocalMergeStartIndex+2*MergeSize;Index++)
				{
					if(Row1Index < Row1End && Row2Index < Row2End)
					{
						if(TmpData[Row1Index] < TmpData[Row2Index])
						{
							MainData[Index] = TmpData[Row1Index];
							Row1Index++;
						}
						else
						{
							MainData[Index] = TmpData[Row2Index];
							Row2Index++;
						}
					}
					else if(Row2Index >= Row2End)
					{
						MainData[Index] = TmpData[Row1Index];
						Row1Index++;
					}
					else if(Row1Index >= Row1End)
					{
						MainData[Index] = TmpData[Row2Index];
						Row2Index++;
					}
				}
			}
			else
			{
				Row1Index = LocalMergeStartIndex;
				Row1End = LocalMergeStartIndex+MergeSize;
				Row2Index = LocalMergeStartIndex+MergeSize;
				Row2End = LocalEnd;
				for(unsigned long Index=LocalMergeStartIndex;Index<LocalEnd;Index++)
				{
					if(Row1Index < Row1End && Row2Index < Row2End)
					{
						if(TmpData[Row1Index] < TmpData[Row2Index])
						{
							MainData[Index] = TmpData[Row1Index];
							Row1Index++;
						}
						else
						{
							MainData[Index] = TmpData[Row2Index];
							Row2Index++;
						}
					}
					else if(Row2Index >= Row2End)
					{
						MainData[Index] = TmpData[Row1Index];
						Row1Index++;
					}
					else if(Row1Index >= Row1End)
					{
						MainData[Index] = TmpData[Row2Index];
						Row2Index++;
					}
				}
			}	
		}
	}
	
	unsigned long FinalLocalMergeStart;
	unsigned long FinalLocalMergeEnd;
	
	for(unsigned long level = 1;level<MB.TotalThreads;level*=2)
	{
		if(x%(level*2) == 0)
		{
			if((x+2*level)*MB.LocalSize < MB.TotalSize)
			{
				FinalLocalMergeStart = x*MB.LocalSize;
				FinalLocalMergeEnd = (x+2*level)*MB.LocalSize;
				Row1Index = FinalLocalMergeStart;
				Row1End = (x+level)*MB.LocalSize;
				Row2Index = (x+level)*MB.LocalSize;
				Row2End = FinalLocalMergeEnd;
				
				for(unsigned long Index = FinalLocalMergeStart;Index<FinalLocalMergeEnd;Index++)
				{
					if(Row1Index < Row1End && Row2Index < Row2End)
					{
						if(MainData[Row1Index] < MainData[Row2Index])
						{
							TmpData[Index] = MainData[Row1Index];
							Row1Index++;
						}
						else
						{
							TmpData[Index] = MainData[Row2Index];
							Row2Index++;
						}
					}
					else if(Row2Index >= Row2End)
					{
						TmpData[Index] = MainData[Row1Index];
						Row1Index++;
					}
					else if(Row1Index >= Row1End)
					{
						TmpData[Index] = MainData[Row2Index];
						Row2Index++;
					}
				}
			}
			else
			{
				FinalLocalMergeStart = x*MB.LocalSize;
				FinalLocalMergeEnd = MB.TotalSize;
				Row1Index = FinalLocalMergeStart;
				Row1End = (x+level)*MB.LocalSize;
				Row2Index = (x+level)*MB.LocalSize;
				Row2End = FinalLocalMergeEnd;
				
				for(unsigned long Index = FinalLocalMergeStart;Index<FinalLocalMergeEnd;Index++)
				{
					if(Row1Index < Row1End && Row2Index < Row2End)
					{
						if(MainData[Row1Index] < MainData[Row2Index])
						{
							TmpData[Index] = MainData[Row1Index];
							Row1Index++;
						}
						else
						{
							TmpData[Index] = MainData[Row2Index];
							Row2Index++;
						}
					}
					else if(Row2Index >= Row2End)
					{
						TmpData[Index] = MainData[Row1Index];
						Row1Index++;
					}
					else if(Row1Index >= Row1End)
					{
						TmpData[Index] = MainData[Row2Index];
						Row2Index++;
					}
				}
			}
		}
		level*=2;
		if(x%(level*2) == 0)
		{
			if((x+2*level)*MB.LocalSize < MB.TotalSize)
			{
				FinalLocalMergeStart = x*MB.LocalSize;
				FinalLocalMergeEnd = (x+2*level)*MB.LocalSize;
				Row1Index = FinalLocalMergeStart;
				Row1End = (x+level)*MB.LocalSize;
				Row2Index = (x+level)*MB.LocalSize;
				Row2End = FinalLocalMergeEnd;
				
				for(unsigned long Index = FinalLocalMergeStart;Index<FinalLocalMergeEnd;Index++)
				{
					if(Row1Index < Row1End && Row2Index < Row2End)
					{
						if(TmpData[Row1Index] < TmpData[Row2Index])
						{
							MainData[Index] = TmpData[Row1Index];
							Row1Index++;
						}
						else
						{
							MainData[Index] = TmpData[Row2Index];
							Row2Index++;
						}
					}
					else if(Row2Index >= Row2End)
					{
						MainData[Index] = TmpData[Row1Index];
						Row1Index++;
					}
					else if(Row1Index >= Row1End)
					{
						MainData[Index] = TmpData[Row2Index];
						Row2Index++;
					}
				}
			}
			else
			{
				FinalLocalMergeStart = x*MB.LocalSize;
				FinalLocalMergeEnd = MB.TotalSize;
				Row1Index = FinalLocalMergeStart;
				Row1End = (x+level)*MB.LocalSize;
				Row2Index = (x+level)*MB.LocalSize;
				Row2End = FinalLocalMergeEnd;
				
				for(unsigned long Index = FinalLocalMergeStart;Index<FinalLocalMergeEnd;Index++)
				{
					if(Row1Index < Row1End && Row2Index < Row2End)
					{
						if(TmpData[Row1Index] < TmpData[Row2Index])
						{
							MainData[Index] = TmpData[Row1Index];
							Row1Index++;
						}
						else
						{
							MainData[Index] = TmpData[Row2Index];
							Row2Index++;
						}
					}
					else if(Row2Index >= Row2End)
					{
						MainData[Index] = TmpData[Row1Index];
						Row1Index++;
					}
					else if(Row1Index >= Row1End)
					{
						MainData[Index] = TmpData[Row2Index];
						Row2Index++;
					}
				}
			}
		}
		
	}
	
}

int main(int argc,char* argv[])
{
	int GridSize,BlockSize,TotalThreads;
	if(argc==3)
	{
		GridSize = atoi(argv[1]);
		BlockSize = atoi(argv[2]);
	}
	else
	{
		GridSize = 1;
		BlockSize = 1;
	}
	TotalThreads = GridSize*BlockSize;
	printf("GridSize:%d,BlockSize:%d\n",GridSize,BlockSize);
	
	
	MessageBlock MainMessageBlock;
	char *HostData;
	char *DeviceData;
	char *TMPDeviceData;
	float Time;
	cudaEvent_t start,end;
	cudaEventCreate(&start);
	cudaEventCreate(&end);
	
	srand(time(NULL));
	
	MainMessageBlock.TotalSize = 1000000000;
	MainMessageBlock.LocalSize = (unsigned long)(MainMessageBlock.TotalSize/TotalThreads);
	MainMessageBlock.TotalThreads = TotalThreads;
	
	HostData = (char*)calloc(MainMessageBlock.TotalSize,sizeof(char));
	
	printf("%lu\n",MainMessageBlock.TotalSize);
	
	for(unsigned long i=0;i<MainMessageBlock.TotalSize;i++)
	{
		HostData[i] = rand()%100;
	}
	/*
	for(unsigned long i=0;i<MainMessageBlock.TotalSize;i++)
	{
		if(HostData[i] < 10) {printf(" ");}
		printf(" %d",HostData[i]);
		if((i+1)%25 == 0) {printf("\n");}
	}
	*/
	cudaMalloc((void**)&DeviceData, MainMessageBlock.TotalSize*sizeof(char));
	cudaMalloc((void**)&TMPDeviceData, MainMessageBlock.TotalSize*sizeof(char));
	
	int Result;
	
	Result = cudaMemcpy(DeviceData,HostData, MainMessageBlock.TotalSize*sizeof(char),cudaMemcpyHostToDevice);
	printf("Host to Device copy result : %d\n",Result);
	cudaEventRecord(start,0);
	Message<<<GridSize,BlockSize>>>(MainMessageBlock,DeviceData,TMPDeviceData);
	cudaEventRecord(end,0);
	cudaEventSynchronize(end);
	cudaEventElapsedTime(&Time,start,end);
	Result = cudaMemcpy(HostData,DeviceData,MainMessageBlock.TotalSize*sizeof(char),cudaMemcpyDeviceToHost);
	printf("Device to Host copy result : %d\n",Result);
	cudaFree(DeviceData);
	cudaFree(TMPDeviceData);
	int CorrectFlag = 1;
	
	unsigned long Errors = 0;
	
	for(unsigned long i=0;i<MainMessageBlock.TotalSize-1;i++)
	{
		if(HostData[i]>HostData[i+1])
		{
			CorrectFlag = 0;
			Errors++;
			//printf("\nError !!! %lu : %d,%d Error !!!\n",i,HostData[i],HostData[i+1]);
		}
	}
	
	printf("Errors : %lu\n",Errors);
	
	/*
	for(unsigned long i=0;i<MainMessageBlock.TotalSize;i++)
	{
		if(HostData[i] < 10) {printf(" ");}
		printf(" %d",HostData[i]);
		if((i+1)%25 == 0) {printf("\n");}
	}
	*/
	
	if(CorrectFlag)
	{
		printf("Congratulations!!!\nNo Fault Occured\n");
	}
	printf("Time Cost : %f\n",Time);
	
	free(HostData);
	
	return 0;
}