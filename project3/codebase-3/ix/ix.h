#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

# define IX_EOF (-1)  // end of the index scan

//# define DEBUG_IX

bool isLessThan(const Attribute *attribute, const void* compValue, const void* compKey);
bool isLessAndEqualThan(const Attribute *attribute, const void* compValue, const void* compKey);
bool isLargerThan(const Attribute *attribute, const void* compValue, const void* compKey);
bool isLargerAndEqualThan(const Attribute *attribute, const void* compValue, const void* compKey);
bool isEqual(const void* value1, const void* value2, const Attribute *attribute);


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
        void printNode(IXFileHandle &ixfileHandle, const Attribute &attribute, const int &pageNum) const;
        RC traverseToLeafWithPath(IXFileHandle &ixfileHandle, Node node, vector<Node*> path,const void *key,const Attribute &attribute);
        RC split(vector<Node*> path, IXFileHandle &ixfileHandle);
        RC split(vector<Node> path, IXFileHandle &ixfileHandle);
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
        RC traverseToLeaf(Node *node);

        IXFileHandle *ixfileHandle;
        const Attribute *attribute;
        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        int cPage;
        int cRec;
        int cKey;
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
    const Attribute * attribute; 
    vector<void *> keys;
    vector<int> children; //pointers to children
    vector<vector<RID>> pointers; //where the record lies
    int next = -1;
    int previous = -1;
    int cPage = -1;
    int order = 2;
    bool isLoaded = false;

    Node(const Attribute *attribute, const void* page);
    Node(const Attribute &attribute);
    Node(const Attribute *attribute);
    ~Node();
    RC serialize(void *page);
    RC insert(void* key, RID rid);
    RC insert(void* key, int child);
    RC insertKey(int pos, const void* key);
    RC insertChild(int pos, int pageNum);
    RC insertPointer(int pos, const RID &rid, const void* key); 
    RC appendKey(const void* key);
    RC appendChild(int pageNum);
    RC appendPointer(vector<RID> rids);
    RC printKeys();
    RC printRids();
    int getChildPos(const void* value);
    // bool isLessThan(const void* compValue, const void* compKey);
};

#endif
