#include "InterNode.hpp"
#include "../Segment/TID.hpp"

template<typename T, typename CMP>
class BTree{
public:
	void insert(T key, TID tid);
	void erase(T key);
	TID lookup(T key);
private:
	BufferManager *bufferManager;
	InterNode<T> *root;
};
