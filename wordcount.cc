#include <omp.h>
#include <iostream>
#include <map>
#include <algorithm>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_WORD_LENGTH 1000
#define MAX_FILE_NUM 10000
#define MAX_FILE_SIZE 2048

using namespace std;

int main(int argc, char* argv[])
{
    char word[MAX_WORD_LENGTH];
    char input_format[100];
	size_t len;
    int i, j;

    int wordCounter = 0;
    int fileCounter = 0;
    long long totalWordCount = 0;
    int fileNum = 0;
    double t[2];

    int td;
    if(argc < 2){
        printf("usage: ./omp tid_num\n");
        exit(0);
    }
    td = atoi(argv[1]);


    vector< vector<string> > token(MAX_FILE_NUM);
    vector< vector<string> > wordMap(MAX_FILE_NUM);
    vector< vector<int> > countMap(MAX_FILE_NUM);
    vector< string > wordStoreTable;
    vector< int > countStoreTable;

	sprintf(input_format, "%%%ds", MAX_WORD_LENGTH);

	while (scanf(input_format,word) == 1)
	{
		len = strlen(word);
		*((size_t*)(word + len)) = 0;
        token[fileCounter].push_back(word);
        wordCounter ++;
        totalWordCount ++;
        if(wordCounter == MAX_FILE_SIZE) {
            wordCounter = 0;
            fileCounter ++;
        }

	}
    if(totalWordCount % MAX_FILE_SIZE == 0)
        fileNum = totalWordCount/MAX_FILE_SIZE;
    else
        fileNum = (totalWordCount/MAX_FILE_SIZE)+1;

    omp_set_num_threads(td);

    t[0] = -omp_get_wtime();
    //mapping
#pragma omp parallel private(i,j) shared(fileNum, token, wordMap,countMap)
{
    #pragma omp for 
    for(i = 0; i < fileNum; i++){
        for(j = 0; j < token[i].size(); j++){
            if(find (wordMap[i].begin(), wordMap[i].end(), token[i][j]) != wordMap[i].end()){
                ptrdiff_t pos = find(wordMap[i].begin(), wordMap[i].end(), token[i][j]) - wordMap[i].begin();
                countMap[i][pos] ++;
            }
               
            else{
                wordMap[i].push_back(token[i][j]);
                countMap[i].push_back(1);
            }
        }
    }
}
    t[0] += omp_get_wtime();
#if 0
    t[1] = -omp_get_wtime();
    // reduce
    for(i = 0; i < fileNum; i++){
        for(j = 0; j < wordMap[i].size(); j++){
            if(find (wordStoreTable.begin(), wordStoreTable.end(), wordMap[i][j]) != wordStoreTable.end()){
                ptrdiff_t pos = find(wordStoreTable.begin(), wordStoreTable.end(), wordMap[i][j]) - wordStoreTable.begin();
                countStoreTable[pos] += countMap[i][j];
            }else{
                wordStoreTable.push_back(wordMap[i][j]);
                countStoreTable.push_back(countMap[i][j]);
            }
        }
    }
    t[1] += omp_get_wtime();
#endif
    printf("The number of thread:%d, Map time:%lf\n", td, t[0]);

    // print out results of wordcount
    //for(i = 0; i < wordStoreTable.size(); i++)
//        cout << wordStoreTable[i] << ":" << countStoreTable[i] << endl;



	return 0;
}
