#include "RAList.h"

RAListNode::RAListNode(void)
{
	data = 0;
}

RAListNode::RAListNode(long data) 
{
	this->data = data;
}

unsigned int RAListNode::hashCode(void)
{
	return (unsigned int)data;
}

bool RAListNode::equals(const RAListNode *another)
{
	return data == another->data;
}

RAListNode* RAListNode::getPrev(void)
{
	if (linkPos.prev == NULL)
		return NULL;
	else
		return (RAListNode*)((char*)linkPos.prev - offsetof(RAListNode, linkPos));
}

RAListNode* RAListNode::getNext(void)
{
	if (linkPos.next == NULL)
		return NULL;
	else
		return (RAListNode*)((char*)linkPos.next - offsetof(RAListNode, linkPos));
}


RAListChunk::RAListChunk(void)
{
	first = NULL;
	size = 0;
}

RAListChunk* RAListChunk::getPrev(void)
{
	if (linkPos.prev == NULL)
		return NULL;
	else
		return (RAListChunk*)((char*)linkPos.prev - offsetof(RAListChunk, linkPos));
}

RAListChunk* RAListChunk::getNext(void)
{
	if (linkPos.next == NULL)
		return NULL;
	else
		return (RAListChunk*)((char*)linkPos.next - offsetof(RAListChunk, linkPos));
}

RAListPart::RAListPart(void)
{
	first = NULL;
	chunks = 0;
	size = 0;
}

RAListPart* RAListPart::getPrev(void)
{
	if (linkPos.prev == NULL)
		return NULL;
	else
		return (RAListPart*)((char*)linkPos.prev - offsetof(RAListPart, linkPos));
}

RAListPart* RAListPart::getNext(void)
{
	if (linkPos.next == NULL)
		return NULL;
	else
		return (RAListPart*)((char*)linkPos.next - offsetof(RAListPart, linkPos));
}

RAList::RAList(const vector<long> &vec, size_t chunkSize)
{
	splitThreshold = chunkSize;
	mergeThreshold = (size_t)(chunkSize * 0.7);
	_size = 0;
	list = allocPart();
	RAListChunk *chunk = allocChunk();
	chunk->part = list;
	list->first = chunk;
	list->chunks = 1;
	for (size_t i = 0; i < vec.size(); i++)
		addHead(vec[i]);
}

RAList::~RAList(void)
{
	// 释放节点
	RAListNode *node = list->first->first;
	while (node != NULL) {
		RAListNode *next = node->getNext();
		freeNode(node);
		node = next;
	}
	// 释放节点片
	RAListChunk *chunk = list->first;
	while (chunk != NULL) {
		RAListChunk *next = chunk->getNext();
		freeChunk(chunk);
		chunk = next;
	}
	// 释放节点分区
	RAListPart *part = list;
	while (part != NULL) {
		RAListPart *next = part->getNext();
		freePart(part);
		part = next;
	}
	list = NULL;
}

void RAList::addHead(long value)
{
	RAListNode *node = allocNode(value);
	addToListHead(node);
	hash.put(node);
	_size++;
}

void RAList::addToListHead(RAListNode *node)
{
	RAListChunk *firstChunk = list->first;
	node->chunk = firstChunk;
	if (firstChunk->first == NULL)
		firstChunk->first = node;
	else {
		firstChunk->first->linkPos.addBefore(&node->linkPos);
		firstChunk->first = node;
	}
	firstChunk->size++;
	list->size++;
	// 若第一个分片过大，分裂成两个片，第一个片一个节点，其余归第二个片
	if (firstChunk->size >= splitThreshold) {
		RAListChunk *newChunk = allocChunk();
		newChunk->part = list;
		newChunk->first = node->getNext();
		newChunk->size = firstChunk->size - 1;
		firstChunk->linkPos.addAfter(&newChunk->linkPos);
		firstChunk->size = 1;
		list->chunks++;

		RAListNode *stop;
		if (newChunk->linkPos.next != NULL)
			stop = newChunk->getNext()->first;
		else
			stop = NULL;
		for (RAListNode *currNode = newChunk->first; currNode != stop; currNode = currNode->getNext()) 
			currNode->chunk = newChunk;
	}
	// 若第一个分区过大，分裂成两个分区，第一个分区一个片，其余归第二个分区
	if (list->chunks >= splitThreshold) {
		RAListPart *newPart = allocPart();
		newPart->first = list->first->getNext();
		newPart->chunks = list->chunks - 1;
		newPart->size = list->size - list->first->size;
		list->linkPos.addAfter(&newPart->linkPos);
		list->chunks = 1;
		list->size = list->first->size;

		RAListChunk *stop;
		if (newPart->linkPos.next != NULL) 
			stop = newPart->getNext()->first;
		else
			stop = NULL;
		for (RAListChunk *currChunk = newPart->first; currChunk != stop; currChunk = currChunk->getNext()) 
			currChunk->part = newPart;
	}
}

bool RAList::remove(long value)
{
	RAListNode key(value);
	RAListNode *node = hash.remove(&key);
	if (node == NULL)
		return false;
	removeFromList(node);
	freeNode(node);
	_size--;
	return true;
}

void RAList::removeFromList(RAListNode *node)
{
	RAListChunk *chunk = node->chunk;
	chunk->size--;
	chunk->part->size--;
	if (node == chunk->first) {
		if (chunk->size > 0)
			chunk->first = node->getNext();
		else
			chunk->first = NULL;
	}
	node->linkPos.unLink();

	// 在当前chunk大小过小时尝试与其左右邻居合并
	RAListChunk *deletedChunk = NULL;
	if (chunk->size < mergeThreshold) {
		if (chunk->getNext() != NULL && (chunk->size == 0 || chunk->size + chunk->getNext()->size < mergeThreshold)) 
			deletedChunk = chunk->getNext();
		else if (chunk->getPrev() != NULL && (chunk->size == 0 || chunk->size + chunk->getPrev()->size < mergeThreshold)) {
			deletedChunk = chunk;
			chunk = deletedChunk->getPrev();
		}
		if (deletedChunk != NULL) {
			RAListNode *stop;
			if (deletedChunk->linkPos.next != NULL)
				stop = deletedChunk->getNext()->first;
			else
				stop = NULL;
			for (RAListNode *currNode = deletedChunk->first; currNode != stop; currNode = currNode->getNext()) 
				currNode->chunk = chunk;
			if (chunk->first == NULL)
				chunk->first = deletedChunk->first;
			chunk->size += deletedChunk->size;
			if (deletedChunk->part != chunk->part) {
				deletedChunk->part->size -= deletedChunk->size;
				chunk->part->size += deletedChunk->size;
			}
		}
	}
	if (deletedChunk != NULL) {
		RAListPart *part = deletedChunk->part;
		part->chunks--;
		if (deletedChunk == part->first)
			part->first = deletedChunk->getNext();
		deletedChunk->linkPos.unLink();
		freeChunk(deletedChunk);

		RAListPart *deletedPart = NULL;
		if (part->chunks < mergeThreshold) {
			if (part->getNext() != NULL && (part->chunks == 0 || part->chunks + part->getNext()->chunks < mergeThreshold))
				deletedPart = part->getNext();
			else if (part->getPrev() != NULL && (part->chunks == 0 || part->chunks + part->getPrev()->chunks < mergeThreshold)) {
				deletedPart = part;
				part = deletedPart->getPrev();
			}
			if (deletedPart != NULL) {
				RAListChunk *stop;
				if (deletedPart->linkPos.next != NULL)
					stop = deletedPart->getNext()->first;
				else
					stop = NULL;
				for (RAListChunk *currChunk = deletedPart->first; currChunk != stop; currChunk = currChunk->getNext())
					currChunk->part = part;
				if (part->first == NULL)
					part->first = deletedPart->first;
				part->chunks += deletedPart->chunks;
				part->size += deletedPart->size;

				deletedPart->linkPos.unLink();
				freePart(deletedPart);
			}
		}
	}
}

bool RAList::moveToHead(long value)
{
	RAListNode key(value);
	RAListNode *v = hash.get(&key);
	if (v == NULL)
		return false;
	removeFromList(v);
	addToListHead(v);
	return true;
}

size_t RAList::getRange(size_t offset, size_t limit, long buf[])
{
	RAListPart *part = list;
	while (part->size <= offset) {
		if (part->linkPos.next == NULL)
			return 0;
		offset -= part->size;
		part = part->getNext();
	}
	RAListChunk *chunk = part->first;
	while (chunk->size <= offset) {
		offset -= chunk->size;
		chunk = chunk->getNext();
	}
	RAListNode *node = chunk->first;
	while (offset > 0) {
		node = node->getNext();
		offset--;
	}
	size_t i = 0;
	while (limit-- > 0) {
		buf[i++] = node->data;
		if (node->linkPos.next == NULL)
			return i;
		node = node->getNext();
	}
	return i;
}

long RAList::operator [](size_t i)
{
	long buf[1];
	if (getRange(i, 1, buf) == 0)
		return -1;
	return buf[0];
}

size_t RAList::size(void) 
{
	return _size;
}

void RAList::check(void)
{
	RAListChunk *chunk;
	RAListNode *node;
	size_t totalSize = 0;
	for (RAListPart *part = list; part != NULL; part = part->getNext()) {
		size_t chunks = 0, partSize = 0;
		chunk = part->first;
		while (chunks < part->chunks) {
			assert(chunk->part == part);
			size_t size = 0;
			node = chunk->first;
			while (size < chunk->size) {
				assert(node->chunk == chunk);
				size++;
				node = node->getNext();
			}
			partSize += chunk->size;
			chunks++;
			chunk = chunk->getNext();
		}
		assert(partSize == part->size);
		totalSize += part->size;
	}
	assert(chunk == NULL);
	assert(node == NULL);

	node = list->first->first;
	while (totalSize > 0) {
		totalSize--;
		node = node->getNext();
	}
	assert(node == NULL);
}

RAListNode* RAList::getHead(void)
{
	return list->first->first;
}

__PS_TLS boost::pool<>* RAList::_node_pool = 0;
__PS_TLS boost::pool<>* RAList::_chunk_pool = 0;
__PS_TLS boost::pool<>* RAList::_part_pool = 0;

void RAList::destroy_pool()
{
	delete _node_pool;
	_node_pool = 0;
	delete _chunk_pool;
	_chunk_pool = 0;
	delete _part_pool;
	_part_pool = 0;
}

RAListNode* RAList::allocNode(long value)
{
	if (_node_pool == 0)
		_node_pool = new boost::pool<>(sizeof(RAListNode));
	return new (_node_pool->malloc()) RAListNode(value);
}

void RAList::freeNode(RAListNode *node)
{
	_node_pool->free(node);
}

RAListChunk* RAList::allocChunk(void)
{
	if (_chunk_pool == 0)
		_chunk_pool = new boost::pool<>(sizeof(RAListChunk));
	return new (_chunk_pool->malloc()) RAListChunk();
}

void RAList::freeChunk(RAListChunk *chunk)
{
	_chunk_pool->free(chunk);
}

RAListPart* RAList::allocPart(void)
{
	if (_part_pool == 0)
		_part_pool = new boost::pool<>(sizeof(RAListPart));
	return new (_part_pool->malloc()) RAListPart();
}

void RAList::freePart(RAListPart *part)
{
	_part_pool->free(part);
}

RAList::iterator::iterator(RAList *list)
{
	node = list->list->first->first;
}

bool RAList::iterator::hasNext(void)
{
	return node != NULL;
}

long RAList::iterator::next(void)
{
	long r = -1;
	if (node) {
		r = node->data;
		node = node->getNext();
	}
	return r;
}

