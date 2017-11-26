#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

# define IX_EOF (-1)  // end of the index scan

// # define DEBUG_IX

bool isLessThan(const Attribute *attribute, const void* compValue, const void* compKey);
bool isLessAndEqualThan(const Attribute *attribute, const void* compValue, const void* compKey);
bool isLargerThan(const Attribute *attribute, const void* compValue, const void* compKey);
bool isLargerAndEqualThan(const Attribute *attribute, const void* compValue, const void* compKey);
bool isEqual(const void* value1, const void* value2, const Attribute *attribute);
int getRidSize(vector<RID> rid);
int getKeySize(const void* key, const Attribute *attribute);

class IX_ScanIterator;
class IXFileHandle;
class Node;
RC writeNodeToPage(IXFileHandle &ixfileHandle, Node *node);    
class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        void printNode(IXFileHandle &ixfileHandle, const Attribute &attribute, const int &pageNum, int indent) const;
        RC traverseToLeafWithPath(IXFileHandle &ixfileHandle, Node *node, vector<Node*> &path,const void *key,const Attribute &attribute);
        Node *traverseToLeafNode(IXFileHandle &ixfileHandle, Node *node, const Attribute &attribute);
        RC split(vector<Node*> path, IXFileHandle &ixfileHandle);
        RC merge(vector<Node*> path, IXFileHandle &ixfileHandle);
        RC doMerge(IXFileHandle &ixfileHandle, Node *nextNode, vector<Node*> path, int &pos, int &direction);
        RC borrow(IXFileHandle &ixfileHandle, Node* node, Node *sibling, Node * parent, int &position, int &direction);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
        int traverseToLeaf(Node *&node);

        IXFileHandle *ixfileHandle;
        const Attribute *attribute;
        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        int cPage;
        int cRec;
        int cKey;
        RID previousRid;
      	int lastPage;;
      	int lastRec;
      	int lastKey;
        Node *node = NULL;
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    FileHandle fileHandle;

};

typedef enum { RootNode = 0, InternalNode, LeafNode, RootOnly } NodeType;

class Node {
public:
    NodeType nodeType;
    AttrType attrType;
    const Attribute *attribute; 
    vector<void *> keys;
    vector<int> children; //pointers to children
    vector<vector<RID>> pointers; //where the record lies
    int next = -1;
    int previous = -1;
    int cPage = -1;
    //int order = 2;
    bool isLoaded = false;
    bool isOverflow = false;
    vector<int> overFlowPages;
    int size = 0;

    Node(const Attribute *attribute, const void* page, IXFileHandle *ixfileHandle);
    Node(const Attribute &attribute);
    Node(const Attribute *attribute);
    ~Node();
    RC serialize(void *page);
    int serializeOverflowPage(int start, int end, void* page);
    RC deserializeOverflowPage(int nodeId, IXFileHandle *ixfileHandle);
    RC insert(void* key, RID rid);
    RC insert(void* key, int child);
    int insertKey(int pos, const void* key);
    RC insertChild(int pos, int pageNum);
    RC insertPointer(int pos, const RID &rid, const void* key); 
    RC appendKey(const void* key);
    RC appendChild(int pageNum);
    RC appendPointer(vector<RID> rids);
    RC printKeys();
    RC printChildren();
    RC printRids(int indent);
    int getNodeSize();
    bool isFull();
    bool isLessHalfFull();
    int getChildPos(const void* value);
    int getKeyPosition(const void *key);
    int getHeaderAndKeysSize();
    RC writeNodeToPage(IXFileHandle &ixfileHandle);
    RC replaceKey(int pos, void *key);
    int deleteRecord(int pos, const RID &rid);
    int findKey(const void* key);
    int getRightSibling(IXFileHandle &ixfileHandle, Node *parent, int &pos);
    int getLeftSibling(IXFileHandle &ixfileHandle, Node *parent, int &pos);
    // bool isLessThan(const void* compValue, const void* compKey);
};

#endif
