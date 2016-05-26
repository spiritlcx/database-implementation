#ifndef H_Basic_HPP
#define H_Basic_HPP

#include "../Parser/Schema.hpp"
#include <iostream>
#include <string>

class Basic{
public:
	static std::string type(const Schema::Relation::Attribute& attr) {
		Types::Tag type = attr.type;
	   	switch(type) {
			case Types::Tag::Integer:
				return to_binary32(0);
	      /*case Types::Tag::Numeric: {
	         std::stringstream ss;
	         ss << "Numeric(" << attr.len1 << ", " << attr.len2 << ")";
	         return ss.str();
	      }*/
	      	case Types::Tag::Char: {
//	         	std::stringstream ss;
				std::string s;
				s += to_binary32(1);
				s += to_binary32(attr.len);
	         	return s;
	      	}
  	 }
  	 throw;
	}

	static std::string to_binary32(unsigned number){
		std::string s;
		s += static_cast<char>(number >> 24 & 0xFF);
		s += static_cast<char>(number >> 16 & 0xFF); 
		s += static_cast<char>(number >> 8 & 0xFF); 
		s += static_cast<char>(number & 0xFF);
		return s;
	}

	static std::string to_binary64(uint64_t number){
		std::string s;
		s += static_cast<char>(number >> 56 & 0xFF);
		s += static_cast<char>(number >> 48 & 0xFF); 
		s += static_cast<char>(number >> 40 & 0xFF); 
		s += static_cast<char>(number >> 32 & 0xFF);
		s += static_cast<char>(number >> 24 & 0xFF);
		s += static_cast<char>(number >> 16 & 0xFF); 
		s += static_cast<char>(number >> 8 & 0xFF); 
		s += static_cast<char>(number & 0xFF);
		return s;		
	}

	static std::string char_to_binary(char c){
		std::string s;
		for(int i = 0; i < 8; i++){
			if(((c >> (7-i)) & 1) == 1){
				s += '1';
			}else{
				s += '0';
			}
		}
		return s;
	}

	static unsigned toUnsigned(char c1, char c2, char c3, char c4){
		std::string s;
		s += char_to_binary(c1);
		s += char_to_binary(c2);
		s += char_to_binary(c3);
		s += char_to_binary(c4);
		unsigned result = std::stoi(s, nullptr, 2);
		return result;
	}
};


#endif
