#include "Operator.hpp"

bool Register::operator==(const Register& other) const{
	switch(type){
		case Types::Tag::Integer:
			return i == other.i;
		case Types::Tag::Char:
			return s == other.s;	
	}
	return false;
}
size_t Register::hashCode() const{
	switch(type){
		case Types::Tag::Integer:
			return i;
		case Types::Tag::Char:
			return std::hash<std::string>()(s);
	}
	return 0;
}

std::ostream& operator<<(std::ostream& os, const Register& r){
	switch(r.getType()){
		case Types::Tag::Integer:
			os << r.i;
			break;
		case Types::Tag::Char:
			os << r.s;
			break;
	}
	return os;	
}


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
		//clear previous tuple
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
	for(const std::vector<Register*> &tuple : tuples){
		for(const Register* r : tuple){
			delete r;
		}
	}

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
	std::vector<Register*> tuple;
	int i = 0;
	int id = 0;
//iterate all attributes of a tuple and add to tuple
	for(const Schema::Relation::Attribute &attribute : relation->attributes){
		Register *r;
		if(attribute.type == Types::Tag::Integer){
			int d = Basic::toInt32(data[i], data[i+1], data[i+2], data[i+3]);
			i += 4;
			r = new Register(d, id);
		}else{
//When attribute is Char, read length characters
			unsigned length = attribute.len;
			std::string d(data, length);
			i += length;
			r = new Register(d, id);
		}
		id++;
		tuple.push_back(r);
	}	
	tuples.push_back(tuple);

	flag = false;
	return tuple;
}

Print::Print(Operator *input, std::ostream &os) : output(os){
	this->input = input;
}

void Print::open(){
	input->open();
}

bool Print::next(){
	while(input->next()){
		std::vector<Register *> tuple = input->getOutput();
		for(Register* r : tuple){
			output << *r << " ";
		}
		output << '\n';
	}
	return false;
}

void Print::close(){
	input->close();
}

Selection::Selection(Operator *input, int id, int constant){
	this->input = input;
	this->id = id;
	this->constant = new Register(constant, 0);
}

Selection::Selection(Operator *input, int id, std::string constant){
	this->input = input;
	this->id = id;
	this->constant = new Register(constant, 0);
}

void Selection::open(){
	input->open();
}

bool Selection::next(){
	while(input->next()){
		std::vector<Register* > r = input->getOutput();
		if(*r[id] == *constant){
			tuple = r;
			flag = true;
			return true;
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

	right->open();
}

bool HashJoin::next(){
//If previous results are not consumed
	if(!tuples.empty())
		return true;
	
	bool flag = false;
	while(right->next()){
		std::vector<Register*> r = right->getOutput();
//For each tuple from right table, there might be multiple results, store all of them
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
	left->close();
	right->close();
}

std::vector<Register*> HashJoin::getOutput(){
	std::vector<Register*> tuple = tuples.front();

	tuples.pop();
	return tuple;
}

void Projection::open(){
	input->open();
}

bool Projection::next(){
	if(input->next())
		return true;
	return false;
}

void Projection::close(){
	input->close();
}

std::vector<Register*> Projection::getOutput(){
	std::vector<Register*> tuple = input->getOutput();
	std::vector<Register*> newtuple;
	for(int id : ids){
		newtuple.push_back(tuple[id]);
	}
	return newtuple;
}
