#include "Operator.hpp"

TableScan::TableScan(const std::string& name){
	this->tableName = name;
}

TableScan::~TableScan(){

}

void TableScan::open(){
	sSegment = new SSegment();
	bufferManager = new BufferManager(100);
	sPSegment = new SPSegment(bufferManager, sSegment, tableName);	

	relation = &sSegment->getTable(tableName);
	numPages = relation->numPages;
	slotCount = sPSegment->getSlotCount(currentPage);
}

//When tuples in a page are consumed, increase currentPage by 1 until the last page is consumed
bool TableScan::next(){
	if(currentSlot < slotCount){
		flag = true;
		return true;
	}else{
		currentPage++;
		if(currentPage < numPages){
			slotCount = sPSegment->getSlotCount(currentPage);
			currentSlot = 0;
			flag = true;
			return true;
		}
	}
	return false;
}

void TableScan::close(){
	delete sSegment;
	delete bufferManager;
	delete sPSegment;	
}

std::vector<Register*> TableScan::getOutput(){
//next must be called before calling this method
	assert(flag);

	Record record = sPSegment->lookup(TID(currentPage, currentSlot));
	currentSlot++;
	const char* data = record.getData();

	std::vector<Register *> tuple;

	int i = 0;
	int id = 0;
//iterate all attributes of a tuple and add to tuple
	for(Schema::Relation::Attribute &attribute : relation->attributes){
		Register *r;
		if(attribute.type == Types::Tag::Integer){
			int d = Basic::toInt32(data[i], data[i+1], data[i+2], data[i+3]);
			i += 4;
			r = new Register(d, id);
		}else{
//When attribute is Char, read length characters
			unsigned length = attribute.len;
			std::string d;
			for(unsigned j = 0; j < length; j++){
				d += data[j+i];
			}
			i += length;
			r = new Register(d, id);
		}
		id++;
		tuple.push_back(r);
	}
	flag = false;
	return tuple;
}

Print::Print(Operator *input){
	this->input = input;
}

void Print::open(){
	input->open();
}

bool Print::next(){
	while(input->next()){
		std::vector<Register *> tuple = input->getOutput();
		for(Register* r : tuple){
			if(r->getType() == Types::Tag::Integer){
				std::cout << r->getInteger()<< " ";
				
			}else{
				std::cout << r->getString();
			}
		}
		std::cout << std::endl;
	}
}

void Print::close(){
	input->close();
}

std::vector<Register*> Print::getOutput(){
	std::vector<Register*> r;
	return r;
}

Selection::Selection(Operator *input, int id, int constant){
	this->input = input;
	this->id = id;
	iconstant = constant;
	type = Types::Tag::Integer;
}

Selection::Selection(Operator *input, int id, std::string constant){
	this->input = input;
	this->id = id;
	sconstant = constant;
	type = Types::Tag::Char;
}

void Selection::open(){
	input->open();
}

bool Selection::next(){
	while(input->next()){
		std::vector<Register* > r = input->getOutput();
		if(type == Types::Tag::Integer){
			if(r[id]->getInteger() == iconstant){
				tuple = r;
				flag = true;
				return true;
			}
		}else{
			if(r[id]->getString() == sconstant){
				tuple = r;
				flag = true;
				return true;
			}
		}
	}
	
	return false;
}

void Selection::close(){
	input->close();
}

std::vector<Register* > Selection::getOutput(){
	assert(flag);
	return tuple;
}


HashJoin::HashJoin(Operator *left, Operator *right, int lid, int rid){
	this->left = left;
	this->right = right;
	this->lid = lid;
	this->rid = rid;
}

void HashJoin::open(){
	left->open();
	while(left->next()){
		std::vector<Register*> tuple = left->getOutput();
		
		index.insert({*tuple[lid], tuple});
	}
	left->close();

	right->open();
}

bool HashJoin::next(){
	if(!tuples.empty())
		return true;
	
	bool flag = false;
	while(right->next()){
		std::vector<Register*> r = right->getOutput();

		auto range = index.equal_range(*r[rid]);
		for(auto it = range.first; it != range.second; it++){
			flag = true;
			std::vector<Register*> tuple;
			for(Register* t : it->second){
				tuple.push_back(t);
			}

			for(Register* t : r){
				tuple.push_back(t);
			}
			tuples.push(tuple);
		}
		if(flag)
			return true;
	}
	return false;
}

void HashJoin::close(){
	right->close();
}

std::vector<Register*> HashJoin::getOutput(){
	std::vector<Register*> tuple = tuples.front();
	tuples.pop();
	return tuple;
}
