#include "Operator.hpp"

TableScan::TableScan(const std::string& name){
	this->tableName = name;
}

TableScan::~TableScan(){
	delete sSegment;
	delete bufferManager;
	delete sPSegment;
}

void TableScan::open(){
	sSegment = new SSegment();
	bufferManager = new BufferManager(100);
	sPSegment = new SPSegment(bufferManager, sSegment, tableName);	
	relation = &sSegment->getTable(tableName);
	numPages = relation->numPages;
	slotCount = sPSegment->getSlotCount(currentPage);
/*	
	for(int i = 0; i < 1000; i++){
		std::string c = "abcd";
		sPSegment->insert(Record(c.length(),c.c_str()));
	}
*/	
}

bool TableScan::next(){
	if(currentSlot < slotCount){
		return true;
	}else{
		currentPage++;
		if(currentPage < numPages){
			slotCount = sPSegment->getSlotCount(currentPage);
			currentSlot = 0;
			return true;
		}
	}
	return false;
}

void TableScan::close(){
	
}

std::vector<Register*> TableScan::getOutput(){
	std::cout << currentPage << " "<< currentSlot << std::endl;
	Record record = sPSegment->lookup(TID(currentPage, currentSlot));
	currentSlot++;
	const char* data = record.getData();
	std::string d(data, record.getLen());
	d += '\0';
//	std::cout << d  << std::endl;
	std::vector<Register *> tuple;
	return tuple;
}
