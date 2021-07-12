#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <omp.h>

void PAUSE(unsigned long code){if(code>0){printf("%lu",code);}fgetc(stdin);}

//TotalSize should be 1000000000 (10^9)
int main(int argc,char* argv[])
{
	char *MainData,*TmpData;
	unsigned long TotalSize,LocalSize;
	double StartTime,EndTime;
	int processor = 1;
	
	unsigned long Row1Index,Row1End;
	unsigned long Row2Index,Row2End;
	
	if(argc>1)
	{
		processor = atoi(argv[1]);
	}
	
	omp_set_num_threads(processor);
	
	TotalSize = 1000000000;
	LocalSize = (unsigned long)(TotalSize/processor);
	
	printf("TotalSize is %d\n",TotalSize);
	
	MainData = calloc(TotalSize,sizeof(char));
	TmpData = calloc(TotalSize,sizeof(char));
	
	for(unsigned long i=0;i<TotalSize;i++)
	{
		MainData[i] = rand()%101;
	}
	
	StartTime = omp_get_wtime();
	for(unsigned long MergeSize=1;MergeSize<TotalSize;MergeSize*=2)
	{
		#pragma omp parallel for shared(MainData,TmpData) private(Row1Index,Row1End,Row2Index,Row2End)
		for(unsigned long LocalMergeStartIndex=0;LocalMergeStartIndex<TotalSize;LocalMergeStartIndex+=2*MergeSize)
		{
			if(LocalMergeStartIndex+2*MergeSize < TotalSize)
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
				Row2End = TotalSize;
				for(unsigned long Index=LocalMergeStartIndex;Index<TotalSize;Index++)
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
		#pragma omp parallel for shared(MainData,TmpData) private(Row1Index,Row1End,Row2Index,Row2End)
		for(unsigned long LocalMergeStartIndex=0;LocalMergeStartIndex<TotalSize;LocalMergeStartIndex+=2*MergeSize)
		{
			if(LocalMergeStartIndex+2*MergeSize < TotalSize)
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
				Row2End = TotalSize;
				for(unsigned long Index=LocalMergeStartIndex;Index<TotalSize;Index++)
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
	
	EndTime = omp_get_wtime();
	
	int Error = 0;
	
	for(unsigned long i=0;i<TotalSize-1;i++)
	{
		if(MainData[i] > MainData[i+1])
		{
			printf("Error at index %lu with data%d,%d\n",i,MainData[i],MainData[i+1]);
			Error++;
		}
		if(Error>100)
		{
			printf("Error too much\n");
			break;
		}
	}
	if(Error == 0)
	{
		/*
		for(int i=0;i<TotalSize;i++)
		{
			if(MainData[i] < 10){printf(" ");}
			printf("%d ",MainData[i]);
			if((i+1)%10 == 0){printf("\n");}
		}
		*/
		printf("No Error\n");
		printf("Time Cost : %f\n",(EndTime-StartTime));
	}
	
	return 0;
}