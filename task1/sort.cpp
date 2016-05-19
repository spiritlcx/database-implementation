#include <errno.h>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <algorithm>
#include <queue>
#include <string.h>
#include <string>
#include <climits>
#include <cmath>

//check if all ways come to an end
bool mergeEnd(int *positions, int k){
	for(int i = 0; i < k; i++)
		if(positions[i] != -1)
			return false;
	return true;
}

void externalSort(int fdInput, uint64_t size, int fdOutput, uint64_t memSize){
	//number of uint64_ts that can be in memory
	int number = memSize / sizeof(uint64_t);

	int k;

	if(size % number == 0){
		k = size / number;
	}else{
		k = size / number + 1;
	}

//at least one uint64_t from each way plus a priority queue and a output buffer can be in memory in merge phase
//number should be >= k+2
	int minimemory = 8 + sqrt(576+256*size) / 2;

	if(number <= k+1){
		std::cerr << "Memory size is too small, at least " << minimemory << " bytes are needed: " << strerror(errno) << std::endl;
		return;
	}

	long readOffset = 0;
	int count = 0;
	uint64_t *buffer = new uint64_t[number];
	int fd, ret;
//indicate which way is being processed
//	int count = 0;
//sort phase
	int numbytes = number*sizeof(uint64_t);

	int sizeRead;
	int sizeWrite;
	while((sizeRead = pread(fdInput, buffer, numbytes, readOffset)) > 0){

		if((fd = open(std::to_string(count).c_str(), O_CREAT|O_TRUNC|O_RDWR, S_IRUSR|S_IWUSR)) < 0){
			std::cerr << "cannot open file '" << std::to_string(k) << "': " << strerror(errno) << std::endl;
			return;
		}
		std::sort(buffer, buffer+sizeRead/sizeof(uint64_t));

		if((ret = posix_fallocate(fd, 0, sizeRead)) != 0){
			std::cerr << "cannot allocate file space: " << strerror(ret) << std::endl;
		}

		if(numbytes != write(fd, buffer, sizeRead)){
			std::cerr << "some bytes are not written to " << std::to_string(k) <<std::endl;
			return;
		}
		count++;
		readOffset += sizeRead;
		close(fd);
	}

	delete [] buffer;
//merge phase
//number of uint64_ts in each way, including the priority_queue
	int numEachWay = number/(k+2);
//number of uint64_t in output buffer, when buffer is full, all uint64_t will be written to disk together, and the buffer becomes empty
	int numResult = number-numEachWay*(k+1);
	uint64_t *outBuffer = new uint64_t[numResult];
	long outOffset = 0;
	int fdsInput[k];
	std::priority_queue<uint64_t,std::vector<uint64_t>, std::greater<uint64_t> > output;
//all uint64_ts that are in memory
	uint64_t **kway = new uint64_t*[k];
	for(int i = 0; i < k; i++)
		kway[i] = new uint64_t[numEachWay];
//current position being processed of each way in memory
	int positions[k];
//number of uint64_t in each way left in memory
	int numEachWays[k];
//offset of each way in files
	long offsets[k];
	memset(positions, 0, k*sizeof(int));
	memset(offsets, 0, k*sizeof(int));
//read numEachWay numbers from k files from disk to memory
	for(int i = 0; i < k; i++){
		fdsInput[i] = open(std::to_string(i).c_str(), O_RDONLY);
		if(fdsInput[i] < 0){
			std::cerr << "cannot open file" << i << std::endl;
		}
		numEachWays[i] = (read(fdsInput[i], kway[i], numEachWay * sizeof(uint64_t)) / sizeof(uint64_t));
		offsets[i] = numEachWays[i] * sizeof(uint64_t);
	}
	
	if((ret = posix_fallocate(fdOutput, 0, size*sizeof(uint64_t))) != 0){
		std::cerr << "cannot allocate file space: " << strerror(ret) << std::endl;
	}

	uint64_t min;
	int kt;
	long writeOffset = 0;
	while(!mergeEnd(positions, k)){
		min = ULLONG_MAX;
		kt = -1;
		for(int i = 0; i < k; i++){
			//-1 means ith way comes to an end
			if(positions[i] == -1){
				continue;
			}

			if(kway[i][positions[i]] < min){
				min = kway[i][positions[i]];
				kt = i;
			}
		}
//when the priority queue is full, move a uint64_t to output buffer
		if(output.size() == numEachWay){
			outBuffer[outOffset++] = output.top();
			output.pop();
		}
// when output buffer is full ,write to disk
		if(outOffset == numResult){
			sizeWrite = pwrite(fdOutput, outBuffer, numResult*sizeof(uint64_t), writeOffset);
		if(sizeWrite != numResult*sizeof(uint64_t)){
			std::cerr << "some bytes are not written to " << std::to_string(k) <<std::endl;
			return;
		}
			writeOffset += numResult * sizeof(uint64_t);
			outOffset = 0;
		}
		output.push(min);

		positions[kt]++;
//when this way comes to an end, read again from file
		if(positions[kt] == numEachWays[kt]){
			if((sizeRead = pread(fdsInput[kt], kway[kt], numEachWay*sizeof(uint64_t), offsets[kt])) != 0){
				positions[kt] = 0;
				numEachWays[kt] = sizeRead / sizeof(uint64_t);
				offsets[kt] += sizeRead;
			}else{
				positions[kt] = -1;
			}
		}
	}

//write the remaining uint64_ts in output buffer and priority queue to disk
	if(outOffset != 0){
		sizeWrite = pwrite(fdOutput, outBuffer, outOffset*sizeof(uint64_t), writeOffset);
		if(sizeWrite != outOffset*sizeof(uint64_t)){
			std::cerr << "some bytes are not written to " << std::to_string(k) <<std::endl;
			return;
		}
		writeOffset += outOffset * sizeof(uint64_t);
	}
	
	delete [] outBuffer;

	while(output.size() != 0){
		sizeWrite = pwrite(fdOutput, &output.top(), sizeof(uint64_t), writeOffset);
		if(sizeWrite != sizeof(uint64_t)){
			std::cerr << "some bytes are not written to " << std::to_string(k) <<std::endl;
			return;
		}
		writeOffset += sizeof(uint64_t);
		output.pop();
	}

	for(int i = 0; i < k; i++){
		delete [] kway[i];
		close(fdsInput[k]);
	}
	delete kway;
}

bool checkSorted(int fdOutput, int memSize){
	long offset = 0;
	int number = memSize / sizeof(uint64_t);
	int numBytes = number * sizeof(uint64_t);
	int sizeRead;
	uint64_t *buffer = new uint64_t[number];

	uint64_t last = 0;
	while((sizeRead = pread(fdOutput, buffer, numBytes, offset)) > 0){
		int numRead = sizeRead / sizeof(uint64_t);
		if(last > buffer[0]){
			return false;
		}
		last = buffer[numRead-1];
		for(int i = 0; i < numRead-1; i++){
			if(buffer[i] > buffer[i+1]){
				return false;
			}
		}
		offset += sizeRead;
	}

	delete [] buffer;
	return true;
}

int main(int argc, char* argv[]){
	if(argc != 4){
		std::cerr << "usage: " << argv[0] << " <inputFile name> <outputFile name> <memoryBufferInMB>" << std::endl;
		return -1;
	}

	double memSize = std::stod(argv[3]);
	if(memSize == 0){
		std::cerr <<  "invalid memory size" << std::endl;
		return -1;
	}

	int fdInput, fdOutput, ret;

	if((fdInput = open(argv[1], O_RDONLY)) < 0){
		std::cerr << "cannot open file '" << argv[1] << "': " << strerror(errno) << std::endl;
		return -1;
	}

	if((fdOutput = open(argv[2], O_CREAT|O_RDWR|O_TRUNC, S_IWUSR|S_IRUSR)) < 0){
		std::cerr << "cannot open file '" << argv[2] << "': " << strerror(errno) << std::endl;
		return -1;
	}
	
	externalSort(fdInput, 625000000, fdOutput, memSize*1000000);

	if(checkSorted(fdOutput, memSize*1000000)){
		std::cout << "Sorted!" <<std::endl;
	}else{
		std::cout << "Not Sorted" << std::endl;
	}

	close(fdInput);
	close(fdOutput);

	return 0;
}
