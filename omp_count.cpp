#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <queue>
#include <algorithm>
#include <cstring>
#include <map>
#include "omp.h"

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

    int i, j;
    
    double t1, t2, t3, t4, t_time;
    double m_td_time[THREAD], r_td_time[THREAD], 
            w_td_time[THREAD], i_td_time[THREAD];

    int fileNum = argc -1;
    int stride1 = fileNum/THREAD;

    map<string, int> mapper[THREAD];
    map<string, int> reducer[THREAD];
    vector<string> words[THREAD];
    int inputSize[THREAD];

    omp_set_num_threads(THREAD);
    t_time = -omp_get_wtime();
    t1 = -omp_get_wtime();
   
    // read in files 
    #pragma omp parallel private(i)
    {
        int tid = omp_get_thread_num();
        int word_count = 0;
        i_td_time[tid] = -omp_get_wtime();
        ifstream inputFile;
        for(i = 0; i < stride1; i++){
            inputFile.open(argv[tid*stride1+i+1]);
            string w;
            while(inputFile >> w){
                transform(w.begin(), w.end(), w.begin(), ::tolower);
                w.erase(std::remove_if(w.begin(), w.end(), charRemove), w.end());
                if(w.size() > 0 && (int)w.at(0) > 60){
                    words[tid].push_back(w);
                    word_count ++;
                }
            }
            inputFile.close();
        }
        inputSize[tid] = word_count;
        i_td_time[tid] += omp_get_wtime();
    }

    t1 += omp_get_wtime();
    t2 = -omp_get_wtime();

    //mapper
    #pragma omp parallel private(i)
    {
        int tid = omp_get_thread_num();
        m_td_time[tid] = -omp_get_wtime();

        for(i = 0; i < inputSize[tid]; i++){
            mapper[tid][words[tid][i]] ++;
        }
        m_td_time[tid] += omp_get_wtime();
    }
    t2 += omp_get_wtime();
    t3 = -omp_get_wtime();

    // shuffering and reduce
    #pragma omp parallel private(i)
    {
        int tid = omp_get_thread_num();
        r_td_time[tid] = -omp_get_wtime();
        map<string, int>::iterator it;
        for(i = 0; i < THREAD; i++){ 
            for(it = mapper[i].begin(); it!= mapper[i].end(); it++){
                if((int)it->first.at(0) > START_TOKEN + tid * STRIDE &&
                    (int)it->first.at(0) <= START_TOKEN + (tid+1) * STRIDE)
                    reducer[tid][it->first] += it->second;
            }
        }
        r_td_time[tid] += omp_get_wtime();
    }
    t3 += omp_get_wtime();
    t4 = -omp_get_wtime();

    //writeOut
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        w_td_time[tid] = -omp_get_wtime();
        fstream outputFile;
        string outFile = "out/result.out";
        ostringstream convert;
        convert << tid;
        outFile.append(convert.str());
        outputFile.open(outFile.c_str(), fstream::out);
        if(reducer[tid].size() > 0){
            map<string, int>::iterator it;
            for(it = reducer[tid].begin(); it!= reducer[tid].end(); it++){
                outputFile << it->first.c_str() << " : " << it->second << endl;
            }
        }
        w_td_time[tid] += omp_get_wtime();
        outputFile.close();
    }

    t4 += omp_get_wtime();
    t_time += omp_get_wtime();
    printf("omp readin:%lf, map:%lf, reduce:%lf, writeout:%lf, total:%lf\n", 
            t1, t2, t3, t4, t_time);
    for(i = 0; i < THREAD; i++){
        printf("readIn[%d]:%lf, mapper[%d]:%lf, reducer[%d]:%lf, writeOut[%d], %lf\n", 
            i, i_td_time[i], i, m_td_time[i], i, r_td_time[i], i, w_td_time[i]);
    }
    return 0;
}
