#include <string>
#include <vector>
#include <unordered_map>
#include <queue>
#include <iostream>
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
	~Register(){}
	int getInteger() const{
		return i;
	}

	std::string getString() const{
		return s;
	}
	
	Types::Tag getType() const{
		return type;
	}
	
	bool operator==(const Register& other) const;
	size_t hashCode() const;

	friend std::ostream& operator<<(std::ostream& os, const Register& r);
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
	virtual std::vector<Register*> getOutput(){};
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

	std::vector<std::vector<Register*> > tuples;
};


class Print : public Operator{
public:
	Print(Operator *input, std::ostream &os);
	virtual void open();
	virtual bool next();
	virtual void close();
private:
	Operator *input;
	std::ostream &output;
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
	Register *constant;
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
	Projection(Operator *input, std::vector<int> ids){
		this->input = input;
		this->ids = ids;
	}
	virtual void open();
	virtual bool next();
	virtual void close();
	virtual std::vector<Register*> getOutput();
private:
	Operator *input;
	std::vector<int> ids;
};

