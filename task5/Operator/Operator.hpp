#include <string>
#include <vector>
#include "../Segment/SPSegment.hpp"

enum class Type {Integer, String};

class Register{
public:
	Register(std::string s, int id){
		this->s = s;
		this->id = id;
		type = Type::String;
	}

	Register(int i, int id){
		this->i = i;
		this->id = id;
		type = Type::Integer;
	}

	int getInteger(){
		return i;
	}

	std::string getString(){
		return s;
	}
	
	Type getType(){
		return type;
	}
private:
	std::string s;
	int i;
	int id;
	Type type;
};

class Operator{
public:
	virtual void open() = 0;
	virtual bool next() = 0;
	virtual void close() = 0;
	virtual std::vector<Register*> getOutput() = 0;
	virtual ~Operator(){}
};

class TableScan : public Operator{
public:
	TableScan(const std::string& name);
	~TableScan();
	virtual void open();
	virtual bool next();
	virtual void close();
	virtual std::vector<Register*> getOutput();
private:
	SPSegment *sPSegment = nullptr;
	SSegment *sSegment = nullptr;
	BufferManager *bufferManager = nullptr;
	std::string tableName;
	Schema::Relation *relation;
	uint64_t numPages;
	uint64_t currentPage = 0;
	uint16_t slotCount;
	uint16_t currentSlot = 0;
};


class Print : public Operator{
public:
	Print(Operator *input);
};

class Projection : public Operator{
public:
	Projection(Operator *input, std::vector<int> ids);
};

class Selection : public Operator{
public:
	Selection(Operator *input, int id, int constant);
};

class HashJoin : public Operator{
public:
	HashJoin(Operator *left, Operator *right, int lid, int rid);
};
