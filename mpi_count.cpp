#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <queue>
#include <algorithm>
#include <cstring>
//#include <string>
#include <map>

#include "mpi.h"

#define MAX_CHAR_LEN 100

using namespace std;

//#define THREAD 2
//#define TOKEN 32
//#define STRIDE (TOKEN/THREAD)
//#define START_TOKEN 96

bool charRemove(int i)
{
    return !std::isalnum(i);
}


int main(int argc, char *argv[]){


	int rank, size;
	//int i, j;
	//int stride1;
	MPI_Status status;
	double t1, t2, t3, t4, t_time;

    ifstream inputFile;

    int fileNum = argc -1;

    map<string, int> reducer[fileNum];
    //vector<string> words[fileNum];
    //vector<int>inputSize;

	MPI_Init(&argc, &argv);
	// process id
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	// the number of process
	MPI_Comm_size(MPI_COMM_WORLD, &size);

    int i, j;
    int *buf;
    int *r_buf;
    int *buf2[size];
    int *r_buf2[size];
    map<string, int> mapper[size];
    map<string, int>::iterator it;
    double m_td_time[size], r_td_time[size], 
            w_td_time[size], i_td_time[size];
    int inputSize[size];
    vector<string> words[size];
    int stride1;

    int THREAD = size;
    int TOKEN = 32;
    int STRIDE = (TOKEN/THREAD);
    int START_TOKEN = 96;
   
    stride1 = fileNum/size;
     
    if(rank == 0) {
        t1 = -MPI_Wtime();
        t_time = -MPI_Wtime();
    }

    i_td_time[rank] = -MPI_Wtime();
    int word_count = 0;
    for(j = 0; j < stride1; j++){
        inputFile.open(argv[rank*stride1+j+1]);
        string w;
        while(inputFile >> w){
            transform(w.begin(), w.end(), w.begin(), ::tolower);
            w.erase(std::remove_if(w.begin(), w.end(), charRemove), w.end());
            if(w.size() > 0 && (int)w.at(0) > 60){
                words[rank].push_back(w);
                word_count ++;
            }
        }
        inputFile.close();
    }
    inputSize[rank] = word_count;

    i_td_time[rank] += MPI_Wtime();

    MPI_Barrier(MPI_COMM_WORLD);

    if(rank == 0) t1 += MPI_Wtime();

    //mapper
    if(rank == 0) {
        t2 = -MPI_Wtime();
    }
    m_td_time[rank] = -MPI_Wtime();
    for(j = 0; j <inputSize[rank]; j++){
        mapper[rank][words[rank][j]] ++;
    }

    if(rank == 0) t2 += MPI_Wtime();
    m_td_time[rank] += MPI_Wtime();
    if(rank == 0) t3 = -MPI_Wtime();
    r_td_time[rank] = -MPI_Wtime();

    // broadcast results of mapper
    int count = 0;
    int global_sum = 0;
    count += (mapper[rank].size());
    MPI_Allreduce(&count, &global_sum, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    buf2[rank] = (int*)calloc(global_sum, sizeof(int));
    buf = (int*)calloc(global_sum*size, sizeof(int));
    char m_str[size][global_sum][MAX_CHAR_LEN];
    char a_str[global_sum*size][MAX_CHAR_LEN];

    count = 0;
    for(it = mapper[rank].begin(); it != mapper[rank].end(); it++){
        buf2[rank][count] = it->second;
        strcpy(m_str[rank][count], it->first.c_str());
        count ++;
    }
    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Allgather(buf2[rank], global_sum , MPI_INT, buf, global_sum, MPI_INT, MPI_COMM_WORLD);
    MPI_Allgather(m_str[rank], global_sum*MAX_CHAR_LEN , MPI_CHAR, a_str, global_sum*MAX_CHAR_LEN, MPI_CHAR, MPI_COMM_WORLD);
    
    MPI_Barrier(MPI_COMM_WORLD);

    // reducer and shuffling
    for(i = 0; i < global_sum * size; i++){
        if(a_str[i] != ""){
            if((int)a_str[i][0] > START_TOKEN + rank * STRIDE &&
                (int)a_str[i][0] <= START_TOKEN + (rank + 1) * STRIDE){
                string abc = a_str[i];
                reducer[rank][abc] += buf[i];
            }
        }
    }
     
    if(rank == 0) t3 += MPI_Wtime();
    r_td_time[rank] += MPI_Wtime();
    MPI_Barrier(MPI_COMM_WORLD);

    if(rank == 0) t4 = -MPI_Wtime();

    w_td_time[rank] = -MPI_Wtime();
    fstream outputFile;
    string outFile = "out/result.out";
    ostringstream convert;
    convert << rank;
    outFile.append(convert.str());
    outputFile.open(outFile.c_str(), fstream::out);

    for(it = reducer[rank].begin(); it!= reducer[rank].end(); it++){
        outputFile << it->first.c_str() << " : " << it->second << endl;
    }
    w_td_time[rank] += MPI_Wtime();

    double i_td_time_final[size];
    double m_td_time_final[size];
    double r_td_time_final[size];
    double w_td_time_final[size];
    MPI_Allgather(&i_td_time[rank], 1 , MPI_DOUBLE, i_td_time_final, 1, MPI_DOUBLE, MPI_COMM_WORLD);
    MPI_Allgather(&m_td_time[rank], 1 , MPI_DOUBLE, m_td_time_final, 1, MPI_DOUBLE, MPI_COMM_WORLD);
    MPI_Allgather(&r_td_time[rank], 1 , MPI_DOUBLE, r_td_time_final, 1, MPI_DOUBLE, MPI_COMM_WORLD);
    MPI_Allgather(&w_td_time[rank], 1 , MPI_DOUBLE, w_td_time_final, 1, MPI_DOUBLE, MPI_COMM_WORLD);
	if(rank == 0){
        t4 += MPI_Wtime();
        t_time += MPI_Wtime();
		printf("mpi readin:%lf, map:%lf, reduce:%lf, writeout:%lf, total time:%lf\n",t1/size, t2, t3, t4, t_time + (t1/size));
        for(i = 0; i < size; i++){
            printf("readIn[%d]:%lf, mapper[%d]:%lf, reducer[%d]:%lf, writeOut[%d]:%lf\n",
                        i, i_td_time_final[i], i, m_td_time_final[i], i, r_td_time_final[i], i, w_td_time_final[i]);
        }
	}
    free(buf2[rank]);
    free(buf);
	MPI_Finalize();
    outputFile.close();
	return 0;
}
