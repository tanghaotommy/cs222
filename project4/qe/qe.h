#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <sstream>
#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"
#include <limits>
#include <unordered_map>

//#define DEBUG_QE

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};

string getOriginalAttrName(const string s);
int getValueOfAttrByName(const void *data, vector<Attribute> &attrs, string attributeName, void* value);
bool compareCondition(const Attribute *attribute, const void* value, const Condition* condition);
void copyAttribute(const void *from, int &fromOffset, void* to, int &toOffset, const Attribute &attr);
void concatenateLeftAndRight(const void* leftData, const void* rightData, void* data, vector<Attribute> &leftAttributes, vector<Attribute> &rightAttributes);
unsigned int hashTypeInt(void* data);
 unsigned int hashTypeReal(void *data);
 unsigned hashTypeVarChar(void *data);
 
struct GroupAttr
{
    float sum = 0;
    float count = 0;
    float max = numeric_limits<float>::min();
    float min = numeric_limits<float>::max();
};

class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    Iterator *input;
    const Condition* condition;
    vector<Attribute> attrs;
};


class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames);   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
        Iterator *input;
        vector<Attribute> attrs;
        vector<Attribute> projectAttrs;
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        vector<Attribute> leftAttrs;
		vector<Attribute> rightAttrs;
        bool firstTuple;
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        );
        ~BNLJoin();
        int count;
        RC getNextTuple(void *data);
        vector<void *> block;
        Iterator *leftInput;
        TableScan *rightInput;
        void *rightTuple;
        void *leftTuple;
        const Condition* condition;
        int numOfPages;
        bool fail;
        // For attribute in vector<Attribute>, name it as rel.attr
        RC loadBlock();
        RC freeBlock();
        bool isConditionEqual();
        void getAttributes(vector<Attribute> &attrs) const;
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin();

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
        
        bool canJoin(const void* leftData, const void* rightData);

        vector<Attribute> leftAttributes;
        vector<Attribute> rightAttributes;
        Iterator *leftIn;
        IndexScan *rightIn;
        string leftTable;
        string rightTable;
        void *leftData = NULL;
        void *leftValue = NULL;
        const Condition* condition;
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );
    ~GHJoin();
    RBFM_ScanIterator *rightRbfm_scanIterator;
    int currentFileHandleIndex;
    Iterator *leftInput;               
    Iterator *rightInput;
    string leftTable;
    string rightTable;
    bool fail;
    RecordBasedFileManager *rbfm;
    vector<Attribute> leftAttributes;
	  vector<Attribute> rightAttributes;
    vector<string> leftAttrNames;
    vector<string> rightAttrNames;
    const Condition* condition;
    unsigned numPartitions;

    unordered_map<int, void *> intMap;
    unordered_map<float, void *> floatMap;
    unordered_map<string, void *> stringMap;

    vector<FileHandle*> leftFileHandles;
    vector<FileHandle*> rightFileHandles;
    vector<vector<vector<void *>>> inPartitionHashMap;
    RC getNextTuple(void *data);
    RC loadLeftPartition();
    RC loadRightPartition();
    RC initRBFM();
    RC partitionFile();
    RC freeinPartitionHashMap();
    bool inPartitionHashMapEquals(void *value1,void *value2, Attribute &attr);
    int inPartitionHashMapFind(void * tuple1, Attribute attr, int index);
    unsigned inPartitionHashFunction(void *key, AttrType type);
    string getNameOfPartition(bool isLeft, int index);
    int HashTuple(void *tuple, bool isLeft);
    unsigned getHashValue(void *tuple, vector<Attribute> Attributes, string hsAttr);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;
    RC setattribute(int type, void *data);
    bool compareAttributeType(int type1, AttrType type2);
    int getAttributeType(void *data);
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );
        ~Aggregate(){};

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;
        string getOpName() const;
        void getAggregateResults();

        Iterator *input;
        vector<Attribute> attrs;
        string relation;
        Attribute aggAttr;
        Attribute groupAttr;
        AggregateOp op;
        bool hasGroupBy = false;
        float sum = 0;
        float count = 0;
        float max = numeric_limits<float>::min();
        float min = numeric_limits<float>::max();
        int current = 0;
        int total = 0;

        unordered_map<string, GroupAttr> stringMap;
        unordered_map<float, GroupAttr> floatMap;
        unordered_map<int, GroupAttr> intMap;

        unordered_map<string, GroupAttr>::iterator stringIterator;
        unordered_map<float, GroupAttr>::iterator floatIterator;
        unordered_map<int, GroupAttr>::iterator intIterator;
};



#endif
