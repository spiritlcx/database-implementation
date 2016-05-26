CXX:=g++
CXXFLAGS:=-std=c++11 -g -O3 -Wall
CXXPTHREAD:=-std=c++11 -g -O3 -Wall -pthread

all: test

BufferReplacement.o: BufferManager/BufferReplacement.hpp BufferManager/BufferReplacement.cpp Makefile
	$(CXX) $(CXXFLAGS) -c BufferManager/BufferReplacement.cpp -o BufferManager/BufferReplacement.o

BufferManager.o: BufferManager/BufferManager.cpp BufferManager/BufferManager.hpp BufferManager/BufferFrame.hpp Makefile
	$(CXX) $(CXXFLAGS) -c BufferManager/BufferManager.cpp -o BufferManager/BufferManager.o

Schema.o: Parser/Schema.cpp Parser/Schema.hpp Makefile
	$(CXX) $(CXXFLAGS) -c Parser/Schema.cpp -o Parser/Schema.o

Parser.o: Parser/Parser.cpp Parser/Parser.hpp Makefile
	$(CXX) $(CXXFLAGS) -c Parser/Parser.cpp -o Parser/Parser.o

SlottedPage.o: Segment/SlottedPage.hpp Segment/SlottedPage.cpp Makefile
	$(CXX) $(CXXFLAGS) -c Segment/SlottedPage.cpp -o Segment/SlottedPage.o

SPSegment.o: Segment/SPSegment.hpp Segment/TID.hpp Segment/SPSegment.cpp Makefile
	$(CXX) $(CXXFLAGS) -c Segment/SPSegment.cpp -o Segment/SPSegment.o

SSegment.o: Segment/SSegment.hpp Segment/SSegment.cpp Makefile
	$(CXX) $(CXXFLAGS) -c Segment/SSegment.cpp -o Segment/SSegment.o

Record.o: Segment/Record.hpp Segment/Record.cpp Makefile
	$(CXX) $(CXXFLAGS) -c Segment/Record.cpp -o Segment/Record.o

BTree.o: Index/BTree.hpp Index/InterNode.hpp Index/LeafNode.hpp Makefile
	$(CXX) $(CXXFLAGS) - c Index/BTree.cpp -o Btree.cpp Makefile

test: Index/BTree.o Segment/Record.o Parser/Schema.o Parser/Parser.o Segment/SPSegment.o Segment/SSegment.o Segment/SlottedPage.o BufferManager/BufferReplacement.o BufferManager/BufferManager.o Parser/Types.hpp slottedTest.cpp Makefile
	$(CXX) $(CXXPTHREAD) Index/BTree.o Segment/Record.o Parser/Schema.o Parser/Parser.o Segment/SPSegment.o Segment/SlottedPage.o Segment/SSegment.o BufferManager/BufferReplacement.o BufferManager/BufferManager.o test.cpp -fsanitize=address -o test

clean:
	rm -rf *.o test
	rm -rf BufferManager/*.o
	rm -rf Segment/*.o
	rm -rf Parser/*.o
