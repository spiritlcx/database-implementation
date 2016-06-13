#include <iostream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <memory>
#include "../Parser/Types.hpp"
#include "SSegment.hpp"
#include "Basic.hpp"

SSegment::SSegment(){
	fid = open("0", O_CREAT|O_RDWR, S_IWUSR|S_IRUSR);

	if(fid == -1){
		std::cerr << errno << std::endl;
	}
	unsigned char *buffer = new unsigned char[PAGESIZE];
	int rid;
	unsigned offset = 0;

	while((rid = pread(fid, buffer, PAGESIZE, offset)) != 0){
		if(rid == -1){
			std::cerr << errno << std::endl;
		}
		
		int i = 0;
		while(i < rid){
			unsigned rel_name_length = Basic::toUnsigned(buffer[i], buffer[i+1], buffer[i+2], buffer[i+3]);
			i+=4;
			std::string tablename;
			for(unsigned j = 0; j < rel_name_length; j++){
				tablename += buffer[i+j];
			}
			Schema::Relation relation(tablename);
			i += rel_name_length;

			unsigned segmentId = Basic::toUnsigned(buffer[i], buffer[i+1], buffer[i+2], buffer[i+3]);
 			i+=4;
			relation.segmentId = segmentId;

			unsigned numPages = Basic::toUnsigned(buffer[i], buffer[i+1], buffer[i+2], buffer[i+3]);
 			i+=4;
			relation.numPages = numPages;

			unsigned attr_length;
			while((attr_length = Basic::toUnsigned(buffer[i], buffer[i+1], buffer[i+2], buffer[i+3])) != 0){
				Schema::Relation::Attribute attribute;
				i+=4;
				std::string attr_name;
				for(unsigned j = 0; j < attr_length; j++){
					attr_name += buffer[i+j];
				}
				attribute.name = attr_name;
				i += attr_length;

				unsigned att_type;
				att_type = Basic::toUnsigned(buffer[i], buffer[i+1], buffer[i+2], buffer[i+3]);
				i+=4;
				// 0 is int, 1 is char, length is needed when char
				if(att_type == 0){
					attribute.type = Types::Tag::Integer;
				}else if(att_type == 1){
					unsigned char_length = Basic::toUnsigned(buffer[i], buffer[i+1], buffer[i+2], buffer[i+3]);
					i += 4;
					attribute.type = Types::Tag::Char;
					attribute.len = char_length;
				}

				relation.attributes.push_back(attribute);
			}
			tables.insert({segmentId, relation});
			segments.insert({tablename, segmentId});
			i+=4;

		}
		offset += PAGESIZE;
	}
}
	
SSegment::SSegment(std::string schema){
	Parser p(schema);
	try{
		std::unique_ptr<Schema> schema = p.parse();
		fid = open("0", O_CREAT|O_RDWR, S_IWUSR|S_IRUSR);
		if(fid == -1){
			std::cout << errno << std::endl;
		}
		std::string s;

		int32_t segmentId = 1;
		for(Schema::Relation &rel : schema->relations){
			Schema::Relation relation(rel.name);
			relation.numPages = 0;
			relation.segmentId = segmentId;
			s += Basic::to_binary32(rel.name.length());
			s += rel.name;
			s += Basic::to_binary32(segmentId);
			//initial size in pages is 0
			s += Basic::to_binary32(0);
			for(Schema::Relation::Attribute &attr : rel.attributes){
				s += Basic::to_binary32(attr.name.length());
				s += attr.name;
				s += Basic::type(attr);
				relation.attributes.push_back(attr);
			}
			tables.insert({segmentId, relation});
			segments.insert({rel.name, segmentId});
			segmentId++;
			//end of a relation
			s += Basic::to_binary32(0);
		}

		if(write(fid, s.c_str(), s.length()) == -1){
			std::cerr << errno << std::endl;
		}

	}catch(ParserError&e){
		std::cout << e.what() << std::endl;
	}
}

SSegment::~SSegment(){

	std::string s;
	for(auto it = tables.begin(); it != tables.end(); it++){
		Schema::Relation& rel = it->second;

		s += Basic::to_binary32(rel.name.length());
		s += rel.name;
		s += Basic::to_binary32(rel.segmentId);
		//initial size in pages is 0
		s += Basic::to_binary32(rel.numPages);
		for(Schema::Relation::Attribute &attr : rel.attributes){
			s += Basic::to_binary32(attr.name.length());
			s += attr.name;
			s += Basic::type(attr);
		}

		//end of a relation
		s += Basic::to_binary32(0);
	}

	if(pwrite(fid, s.c_str(), s.length(), 0) == -1){
		std::cerr << errno << std::endl;
	}

	close(fid);
}

Schema::Relation &SSegment::getTable(const std::string& tableName){
	auto segmentId = segments.find(tableName);
	auto it = tables.find(segmentId->second);
	return it->second;
}

uint16_t SSegment::getSegment(const std::string& tableName){
	auto it = segments.find(tableName);
	return it->second;
}
