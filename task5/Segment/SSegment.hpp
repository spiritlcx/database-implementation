#include "../Parser/Parser.hpp"
#include "../Parser/Schema.hpp"
#include "../BufferManager/BufferManager.hpp"
#include <unordered_map>
#include <string>

class SSegment{
public:
	SSegment();
	SSegment(std::string schema);
	~SSegment();
	Schema::Relation &getTable(const std::string& tableName);
	uint16_t getSegment(const std::string& tableName);
private:
	int fid;
	std::unordered_map<uint16_t, Schema::Relation> tables;
	std::unordered_map<std::string, uint16_t> segments;
};
