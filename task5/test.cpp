#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "Operator/Operator.hpp"
using namespace std;

std::vector<std::string> split(std::string s, char delimit){
	std::vector<std::string> names;
	std::size_t begin = 0;
	std::size_t end;
	while((end = s.find(delimit, begin)) != std::string::npos){
		names.push_back(s.substr(begin, end - begin));
		begin = end + 1;
	}
	names.push_back(s.substr(begin, s.size()-begin));
	return names;
}

struct Order{
	Order() {name = "order";}
	struct Row{
		Integer o_id;
		Integer o_d_id;
		Integer o_w_id;
		std::string o_entry_d;
	};

	unsigned size(){return orders.size();}
	std::string name;
	std::vector<Row> orders;
	void init(){
		std::string path = "tpcc_order.tbl";
		std::ifstream f(path);
		std::string line;

		while(!f.eof() && f >> line){
			std::vector<std::string> data = split(line, '|');
			Row row;
			row.o_id = Integer::castString(data[0].c_str(), data[0].length());
			row.o_d_id = Integer::castString(data[1].c_str(), data[1].length());
			row.o_w_id = Integer::castString(data[2].c_str(), data[2].length());
			row.o_entry_d = data[4].c_str();
			orders.push_back(row);
		}
	}
	
};

struct NewOrder{
	NewOrder() {name = "neworder";}
	struct Row{
		Integer no_o_id;
		Integer no_d_id;
		Integer no_w_id;
	};

	unsigned size(){return neworders.size();}

	std::string name;
	std::vector<Row> neworders;

	void init(){

		std::string path = "tpcc_neworder.tbl";
		std::ifstream f(path);
		std::string line;

		while(!f.eof() && f >> line){
			std::vector<std::string> data = split(line, '|');
			Row row;
			row.no_o_id =Integer::castString(data[0].c_str(), data[0].length());
			row.no_d_id =Integer::castString(data[1].c_str(), data[1].length());
			row.no_w_id =Integer::castString(data[2].c_str(), data[2].length());
			neworders.push_back(row);
		}
	}
};

//Create database, table and load data to the database
class PreProcess{
public:
	PreProcess(){
		sSegment = new SSegment("schema.sql");
		bufferManager = new BufferManager(1000);
		buildOrder();
		buildNewOrder();
	}
	~PreProcess(){
		delete sSegment;
		delete bufferManager;
	}
	void buildOrder(){
		Order order;
		order.init();

		SPSegment *segment = new SPSegment(bufferManager, sSegment, "order");
		for(unsigned i = 0; i < order.size(); i++){
			std::string s;
			s += Basic::to_binaryInt32(order.orders[i].o_id.value);
			s += Basic::to_binaryInt32(order.orders[i].o_d_id.value);
			s += Basic::to_binaryInt32(order.orders[i].o_w_id.value);

			if(order.orders[i].o_entry_d.length() == 1){
				s += order.orders[i].o_entry_d[0];
				s += ' ';
			}else{
				s += order.orders[i].o_entry_d[0];
				s += order.orders[i].o_entry_d[1];
			}
			segment->insert(Record(s.length(), s.c_str()));
		}
		delete segment;
	}

	void buildNewOrder(){
		NewOrder neworder;
		neworder.init();

		SPSegment *segment = new SPSegment(bufferManager, sSegment, "neworder");
		for(unsigned i = 0; i < neworder.size(); i++){
			std::string s;
			s += Basic::to_binaryInt32(neworder.neworders[i].no_o_id.value);
			s += Basic::to_binaryInt32(neworder.neworders[i].no_d_id.value);
			s += Basic::to_binaryInt32(neworder.neworders[i].no_w_id.value);

			segment->insert(Record(s.length(), s.c_str()));
		}
		delete segment;
	}
private:
	BufferManager *bufferManager;
	SSegment *sSegment;
};

int main(){
//crate database and load data
	PreProcess* pre = new PreProcess();
	delete pre;

//select 1 on column 1
	TableScan order("order");
	Selection selection(&order, 1, 1);

//project only column 0
	TableScan neworder("neworder");
	std::vector<int> ids;
	ids.push_back(0);
	Projection projection(&neworder, ids);

	HashJoin hashJoin(&selection, &projection, 0, 0);

	Print print(&hashJoin, std::cout);
	print.open();
	print.next();	
	print.close();

	return 0;
}
