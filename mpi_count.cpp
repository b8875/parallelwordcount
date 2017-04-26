#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <queue>
#include <algorithm>
#include <cstring>
#include <map>

#include "mpi.h"

#define MAX_CHAR_LEN 100

using namespace std;

#define THREAD 16
#define TOKEN 32
#define STRIDE (TOKEN/THREAD)
#define START_TOKEN 96

bool charRemove(int i)
{
    return !std::isalnum(i);
}


int main(int argc, char *argv[]){


	int rank, size;
	int i, j;
	int stride1;
	int start, end;
	int t = 0;
	int result = 0;
	int source, dest;
	int tag1, tag2;
	MPI_Status status;
	double t1, t2;

    ifstream inputFile;

    int fileNum = argc -1;

    //map<string, int> mapper[fileNum];
    map<string, int> reducer[fileNum];
    vector<string> words[fileNum];
    vector<int>inputSize;

	MPI_Init(&argc, &argv);
	// process id
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	// the number of process
	MPI_Comm_size(MPI_COMM_WORLD, &size);

    int *buf;
    int *buf2[size];
    map<string, int> mapper[size];
	tag2 = 1;
	tag1 = 2;
    
    // read input files
    for(i = 1; i < argc; i++){
        inputFile.open(argv[i]);
        string w;
        int word_count = 0;
        while(inputFile >> w){
            transform(w.begin(), w.end(), w.begin(), ::tolower);
            w.erase(std::remove_if(w.begin(), w.end(), charRemove), w.end());
            if(w.size() > 0 && (int)w.at(0) > 60){
                words[i-1].push_back(w);
                word_count ++;
            }
        }
        inputFile.close();
        inputSize.push_back(word_count);

    }
    stride1 = fileNum/size;

    MPI_Barrier(MPI_COMM_WORLD);
    //mapper
    if(rank == 0) t1 = -MPI_Wtime();

    for(i = 0; i < stride1; i++){
        for(j = 0; j <inputSize[rank * stride1 + i]; j++){
            mapper[rank][words[rank * stride1 + i][j]] ++;
        }
    }
    
    // broadcast results of mapper
    map<string, int>::iterator it;
    int count = 0;
    int global_sum = 0;
    count += (mapper[rank].size());
    buf2[rank] = (int*)malloc(sizeof(int)*count);
    MPI_Allreduce(&count, &global_sum, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    buf = (int*)malloc(sizeof(int)*global_sum*size);
    char m_str[global_sum][MAX_CHAR_LEN];
    char a_str[global_sum*size][MAX_CHAR_LEN];

    count = 0;
    for(it = mapper[rank].begin(); it != mapper[rank].end(); it++){
        buf2[rank][count] = it->second;
        strcpy(m_str[count], it->first.c_str());
        count ++;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if(rank == 0) t1 += MPI_Wtime();
            
    MPI_Allgather(buf2[rank], global_sum , MPI_INT, buf, global_sum, MPI_INT, MPI_COMM_WORLD);
    MPI_Allgather(m_str, global_sum*MAX_CHAR_LEN , MPI_CHAR, a_str, global_sum*MAX_CHAR_LEN, MPI_CHAR, MPI_COMM_WORLD);
    mapper[rank].clear();
    MPI_Barrier(MPI_COMM_WORLD);

    // reducer and shuffling
    if(rank == 0) t2 = -MPI_Wtime();
    for(i = 0; i < global_sum * size; i++){
        if(buf[i] > 0){
            if((int)a_str[i][0] > START_TOKEN + rank * STRIDE &&
                (int)a_str[i][0] <= START_TOKEN + (rank + 1) * STRIDE){
                string abc = a_str[i];
                reducer[rank][abc] += buf[i];
            }
        }
    }
    if(rank == 0) t2 += MPI_Wtime();
    //if(rank == 0 )printf("mapper size:%d, %d, %d\n", reducer[0]["a"], buf[1], count);

    MPI_Barrier(MPI_COMM_WORLD);

#if 0
    // write out word count results
    if(rank == 0){
        map<string, int>::iterator it;
        for(it = reducer[rank].begin(); it!= reducer[rank].end(); it++){
             printf("file size:%s, %d\n", it->first.c_str(), it->second);
        }
    }
#endif        
	if(rank == 0){
		printf("mapper time:%lf, reduce%lf\n",t1, t2);

	}
    free(buf2[rank]);
	MPI_Finalize();
	return 0;
}
