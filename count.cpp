#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
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
    vector<int>inputSize;
    vector< vector< string > > words;
    ifstream inputFile;
    
    map<int, map<string, int> > mapper;
    map<int, map<string, int> > reducer;
    double t1, t2, t3;

    t1 = -omp_get_wtime();   
    // read in files
    for(i = 1; i < argc; i++){
        inputFile.open(argv[i]);
        string w;
        int word_count = 0;
        words.push_back(vector<string>());
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
    t1 += omp_get_wtime();

    t2 = -omp_get_wtime();
    // mappers
    for(i = 0; i < argc-1; i++){
        for(j = 0; j < inputSize[i]; j++){
            mapper[i][words[i][j]] ++;;
        }
    }
    t2 += omp_get_wtime();
   
    t3 = -omp_get_wtime();
    // shuffering and reduce
    for(i = 0; i < THREAD; i++){ 
        for(j = 0; j < argc-1; j++){
            map<string, int>::iterator it; 
            for(it = mapper[j].begin(); it!= mapper[j].end(); it++){
                if((int)it->first.at(0) > START_TOKEN + i * STRIDE &&
                    (int)it->first.at(0) <= START_TOKEN + (i+1) * STRIDE)
                    reducer[i][it->first] += it->second;
            }
        }
    }
    t3 += omp_get_wtime();
#if 0
    for(i = 0; i < THREAD; i++){
        if(reducer[i].size() > 0){
            map<string, int>::iterator it;
            for(it = reducer[i].begin(); it!= reducer[i].end(); it++){
                printf("file size:%s, %d\n", it->first.c_str(), it->second);
            }
        }
    }
#endif
    printf("read:%lf, map:%lf, reduce:%lf\n", t1, t2, t3);
    return 0;
}
