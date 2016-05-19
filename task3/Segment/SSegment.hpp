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
	Schema::Relation &getTable(uint16_t segmentId);
private:
	int fid;
	std::unordered_map<uint16_t, Schema::Relation> tables;
};
