
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstring>
#include <cmath>
#include <unordered_map>
#include <algorithm>
#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
int getKey(const string &tableName,const vector<Attribute> recordDescriptor, const void *data, string keyName, void *key, int &length);

class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
  RBFM_ScanIterator rbfm_ScanIterator;
  FileHandle fileHandle;  
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() {};    // Constructor
  ~RM_IndexScanIterator();   // Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);    // Get next matching entry
  RC close();                  // Terminate index scan

  IX_ScanIterator ix_ScanIterator;
  IXFileHandle *ixfileHandle = NULL;
  vector<Attribute> attrs;
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();
  unordered_map<string, vector<Attribute>> HashMap;
  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);
  RC getTableId(const string &tableName, int &tableId);
  RC prepareCatalogTableDescriptor(vector<Attribute> &attributes);
  RC prepareCatalogColumnDescriptor(vector<Attribute> &attributes);
  RC prepareTablesRecord(const vector<Attribute> &recordDescriptor, void *data,int tableid,const string tablename,int isSystemTable);
  RC prepareColumnsRecord(const vector<Attribute> &recordDescriptor, void *data,int tableid,Attribute attr, int position,int isDeleted);
  RC getFileNameByTableName(const string &tableName, string &fileName);
  RC insertColumn(int tableid, const vector<Attribute> &attributes);
  RC UpdateColumns(int tableid,vector<Attribute> attributes);
  int isSystemTable(const string &tableName);
  int generateNextTableId();
  RC getAllAttributes(const string &tableName, vector<Attribute> &attrs);
  RC removeNonExisted(const string &tableName, void* data);
  int getSizeOfdata(vector<Attribute> &attr, void* data);
  RC getIndexAttrNames(const string &tableName, vector<string>& indexAttrNames);
  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  RC insertIndex(const string &tableName, const vector<Attribute> recordDescriptor, const void *data, const RID &rid);
  RC insertSingleIndex(const string &tableName, const vector<Attribute> recordDescriptor, const string indexName, const void *data, const RID &rid);

  RC deleteIndex(const string &tableName, const vector<Attribute> recordDescriptor, const void *data, const RID &rid);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);


protected:
  RelationManager();
  ~RelationManager();
  RecordBasedFileManager* rbfm;
};

#endif
