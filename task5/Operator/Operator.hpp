#include <string>
#include <vector>
#include <unordered_map>
#include <queue>
#include "../Segment/SPSegment.hpp"

class Register{
public:
	Register(std::string s, int id){
		this->s = s;
		this->id = id;
		type = Types::Tag::Char;
	}

	Register(int i, int id){
		this->i = i;
		this->id = id;
		type = Types::Tag::Integer;
	}

	int getInteger(){
		return i;
	}

	std::string getString(){
		return s;
	}
	
	Types::Tag getType(){
		return type;
	}
	
	bool operator==(const Register& other) const{
		switch(type){
			case Types::Tag::Integer:
				return i == other.i;
			case Types::Tag::Char:
				return s == other.s;	
		}
	}
	size_t hashCode() const{
		switch(type){
			case Types::Tag::Integer:
				return i;
			case Types::Tag::Char:
				return std::hash<std::string>()(s);
		}
	}
private:
	std::string s;
	int i;
	int id;
	Types::Tag type;
};

namespace std{
	template<>
	struct hash<Register>{
		size_t operator()(const Register& k) const{
			return k.hashCode();
		}
	};
}

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
	bool flag = false;
};


class Print : public Operator{
public:
	Print(Operator *input);
	virtual void open();
	virtual bool next();
	virtual void close();
	virtual std::vector<Register*> getOutput();
private:
	Operator *input;
};

class Projection : public Operator{
public:
	Projection(Operator *input, std::vector<int> ids);
};

class Selection : public Operator{
public:
	Selection(Operator *input, int id, int constant);
	Selection(Operator *input, int id, std::string constant);
	virtual void open();
	virtual bool next();
	virtual void close();
	virtual std::vector<Register*> getOutput();
private:
	int id;
	Operator *input;
	int iconstant;
	std::string sconstant;
	Types::Tag type;
	std::vector<Register*> tuple;
	bool flag =false;
};

class HashJoin : public Operator{
public:
	HashJoin(Operator *left, Operator *right, int lid, int rid);
	virtual void open();
	virtual bool next();
	virtual void close();
	virtual std::vector<Register*> getOutput();
private:
	Operator *left;
	Operator *right;
	int lid;
	int rid;
	std::unordered_multimap<Register, std::vector<Register*>> index;
	std::queue<std::vector<Register*> > tuples;
	Types::Tag type;
};

class Projection : public Operator{
public:
	Projection(std::vector<int> ids){
		this->ids = ids;
	}
	virtual void open();
	virtual bool next();
	virtual void close();
private:
	Operator *input;
	std::vector<int> ids;
};
