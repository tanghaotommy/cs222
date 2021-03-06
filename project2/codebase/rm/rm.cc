
#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
	rbfm = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    vector<Attribute> tablesdescriptor;
	vector<Attribute> columndescriptor;

	FileHandle table_filehandle;
	RID rid;

	if((rbfm->createFile("Tables"))!=0){
		return -1;
	}
	int tableId = 1;
	rbfm->openFile("Tables",table_filehandle);
	void *data = malloc(PAGE_SIZE);
	prepareCatalogTableDescriptor(tablesdescriptor);

	prepareTablesRecord(tablesdescriptor, data,tableId,"Tables",1);
	// rbfm->printRecord(tablesdescriptor, data);
	int nFields = tablesdescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memcpy(&nullFieldsIndicator,(char *)data,nullFieldsIndicatorActualSize);
	//rbfm->printRecord(tablesdescriptor,data);	
	RC rc = rbfm->insertRecord(table_filehandle,tablesdescriptor,data,rid);
	if(rc != 0) return -1;
	free(data);

	tableId = 2;
	void *data2 = malloc(PAGE_SIZE);
	prepareTablesRecord(tablesdescriptor,data2,tableId,"Columns",1);
	nFields = tablesdescriptor.size();
    nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	memcpy(&nullFieldsIndicator,(char *)data2,nullFieldsIndicatorActualSize);
	//rbfm->printRecord(tablesdescriptor, data2);
	rc = rbfm->insertRecord(table_filehandle, tablesdescriptor,data2,rid);

	if(rc != 0) return -1;
	free(data2);
	rbfm->closeFile(table_filehandle);

	if(rbfm->createFile("Columns") != 0){
		return -1;
	}
	insertColumn(1,tablesdescriptor);
	prepareCatalogColumnDescriptor(columndescriptor);
	insertColumn(2,columndescriptor);
	return 0;
}

RC RelationManager::deleteCatalog()
{
	if(rbfm->destroyFile("Tables")==0){
		if(rbfm->destroyFile("Columns")==0){
			return 0;
		}
	}
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle filehandle;
	vector<Attribute> tablesdescriptor;
	void *data = (char *)malloc(PAGE_SIZE);

	if(rbfm->createFile(tableName) != 0){
		return -1;
	}
	if(rbfm->openFile("Tables", filehandle)!=0){
		return -1;
	}
	//static int tableId = 2;
	//tableId++;

	int tableId = generateNextTableId();

	RID rid;
	prepareCatalogTableDescriptor(tablesdescriptor);
	prepareTablesRecord(tablesdescriptor, data,tableId,tableName,0);
	//rbfm->printRecord(tablesdescriptor,data);
	RC rc = rbfm->insertRecord(filehandle,tablesdescriptor,data,rid);
	free(data);
	if(rc != 0) return -1;
	rbfm->closeFile(filehandle);
	rc = insertColumn(tableId, attrs);

	if (rc != 0) return -1;
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	if(isSystemTable(tableName) != 0) return -1;
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	FileHandle fileHandle;
	if(rbfm->openFile("Tables", fileHandle) != 0)
		return -1;
	vector<Attribute> recordDescriptor;
	prepareCatalogTableDescriptor(recordDescriptor);
	string conditionAttribute = "table-name";

	int nameLength = tableName.length();
	char *value = (char *)malloc(tableName.length() + sizeof(int));
	memcpy(value, &nameLength, sizeof(int));
	memcpy(value + sizeof(int), tableName.c_str(), tableName.length());
	vector<string> attributeNames;
	string attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "table-name";
	attributeNames.push_back(attributeName);
	attributeName = "file-name";
	attributeNames.push_back(attributeName);
	attributeName = "system-table";
	attributeNames.push_back(attributeName);
	CompOp compOp = EQ_OP;
	RBFM_ScanIterator rbfm_ScanIterator;

	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);

	RID rid;
	void *data = malloc(PAGE_SIZE);
	string fileName;
	int tableId;

	if(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		int offset = 1;
		// rbfm->printRecord(recordDescriptor, data);
		memcpy(&tableId, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		int nameLength;
		memcpy(&nameLength, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		char *fileName_c = (char*) malloc(nameLength + 1);
		memcpy(fileName_c, (char *)data + offset, nameLength);
		fileName_c[nameLength] = '\0';
		fileName = string(fileName_c);
		// printf("nameLength: %d, fileName: %s, fileName_c: %s, tableId: %d\n", nameLength, fileName.c_str(), fileName_c, tableId);
	}
	else return -1;
	rbfm_ScanIterator.close();

	rbfm->destroyFile(fileName);

	conditionAttribute = "table-id";
	realloc(value, sizeof(int));
	memcpy(value, &tableId, sizeof(int));
	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
	if(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		// rbfm->printRecord(recordDescriptor, data);
		//printf("slotNum: %d, pageNum: %d\n", rid.slotNum, rid.pageNum);
		rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
	}
	else
		return -1;

	
	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);
	
	rbfm->openFile("Columns", fileHandle);
	recordDescriptor.clear();
	
	prepareCatalogColumnDescriptor(recordDescriptor);
	attributeNames.clear();
	attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "column-name";
	attributeNames.push_back(attributeName);
	attributeName = "column-type";
	attributeNames.push_back(attributeName);
	attributeName = "column-length";
	attributeNames.push_back(attributeName);
	attributeName = "column-position";
	attributeNames.push_back(attributeName);
	attributeName = "delete-flag";
	attributeNames.push_back(attributeName);

	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
	while(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		// rbfm->printRecord(recordDescriptor, data);
		rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
	}
	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);
	free(data);
	free(value);
	return 0;
}

bool sortByPosition(const pair<int, Attribute> &a,
          const pair<int, Attribute> &b)
{
    return (a.first < b.first);
}

RC RelationManager::getAllAttributes(const string &tableName, vector<Attribute> &attrs)
{
	int tableId;
	if(this->getTableId(tableName, tableId) != 0)
		return -1;
	// printf("TableId: %d\n", tableId);

	vector<Attribute> recordDescriptor;
	prepareCatalogColumnDescriptor(recordDescriptor);
	string conditionAttribute = "table-id";
	void *value = malloc(sizeof(int));
	memcpy(value, &tableId, sizeof(int));
	vector<string> attributeNames;
	
	attributeNames.push_back("table-id");
	attributeNames.push_back("column-name");
	attributeNames.push_back("column-type");
	attributeNames.push_back("column-length");
	attributeNames.push_back("column-position");
	attributeNames.push_back("delete-flag");
	
	CompOp compOp = EQ_OP;
	RM_ScanIterator rm_ScanIterator;

	this->scan("Columns", conditionAttribute, compOp, value, attributeNames, rm_ScanIterator);

	RID rid;
	void *data = malloc(PAGE_SIZE);
	string fileName;

	vector<Attribute> attributes;
	vector< pair <int, Attribute> > vect;
	while(rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
	{
		Attribute attr;
		int offset = ceil((double) attributeNames.size() / CHAR_BIT);
		// rbfm->printRecord(recordDescriptor, data);
		memcpy(&tableId, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		int nameLength;
		memcpy(&nameLength, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		char *fileName_c = (char*) malloc(nameLength + 1);
		memcpy(fileName_c, (char *)data + offset, nameLength);
		fileName_c[nameLength] = '\0';
		fileName = string(fileName_c);
		offset += nameLength;
		AttrType columnType;
		memcpy(&columnType, (char *)data + offset, sizeof(AttrType));
		offset += sizeof(AttrType);
		int columnLength;
		memcpy(&columnLength, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		int columnPosition;
		memcpy(&columnPosition, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		int isDeleted;
		memcpy(&isDeleted, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		
#ifdef DEBUG
		printf("nameLength: %d, fileName: %s, fileName_c: %s, tableId: %d, type: %d, column-length:%d, column-position: %d\n", 
			nameLength, fileName.c_str(), fileName_c, tableId, columnType, columnLength, columnPosition);
#endif

		attr.name = fileName;
		attr.type = columnType;
		attr.length = columnLength;
		//if(isDeleted != 0) attr.length = 0;
		vect.push_back(make_pair(columnPosition, attr));
		// attrs.push_back(attr);
	}
	rm_ScanIterator.close();

	sort(vect.begin(), vect.end(), sortByPosition);
	
	for (int i = 0; i < vect.size(); ++i)
	{
		attrs.push_back(vect[i].second);
	}
	// printf("%s\n", attrs[3].name.c_str());
	// else return -1;
	free(data);
	free(value);
	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	int tableId;
	if(this->getTableId(tableName, tableId) != 0)
		return -1;

	vector<Attribute> recordDescriptor;
	prepareCatalogColumnDescriptor(recordDescriptor);
	string conditionAttribute = "table-id";
	void *value = malloc(sizeof(int));
	memcpy(value, &tableId, sizeof(int));
	vector<string> attributeNames;
	
	attributeNames.push_back("table-id");
	attributeNames.push_back("column-name");
	attributeNames.push_back("column-type");
	attributeNames.push_back("column-length");
	attributeNames.push_back("column-position");
	attributeNames.push_back("delete-flag");
	
	CompOp compOp = EQ_OP;
	RM_ScanIterator rm_ScanIterator;

	this->scan("Columns", conditionAttribute, compOp, value, attributeNames, rm_ScanIterator);

	RID rid;
	void *data = malloc(PAGE_SIZE);
	string fileName;

	vector<Attribute> attributes;
	vector< pair <int, Attribute> > vect;
	while(rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
	{
		Attribute attr;
		int offset = ceil((double) attributeNames.size() / CHAR_BIT);
		// rbfm->printRecord(recordDescriptor, data);
		memcpy(&tableId, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		int nameLength;
		memcpy(&nameLength, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		char *fileName_c = (char*) malloc(nameLength + 1);
		memcpy(fileName_c, (char *)data + offset, nameLength);
		fileName_c[nameLength] = '\0';
		fileName = string(fileName_c);
		offset += nameLength;
		AttrType columnType;
		memcpy(&columnType, (char *)data + offset, sizeof(AttrType));
		offset += sizeof(AttrType);
		int columnLength;
		memcpy(&columnLength, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		int columnPosition;
		memcpy(&columnPosition, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		int isDeleted;
		memcpy(&isDeleted, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		
		attr.name = fileName;
		attr.type = columnType;
		attr.length = columnLength;
		if(isDeleted == 0) vect.push_back(make_pair(columnPosition, attr));;
		
		// attrs.push_back(attr);
	}
	rm_ScanIterator.close();

	sort(vect.begin(), vect.end(), sortByPosition);
	
	for (int i = 0; i < vect.size(); ++i)
	{
		attrs.push_back(vect[i].second);
	}
	free(value);
	free(data);
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if(isSystemTable(tableName) != 0) return -1;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();	
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	string fileName;
	if (getFileNameByTableName(tableName, fileName) != 0)
		return -1;
	getAttributes(tableName, recordDescriptor);
	if(rbfm->openFile(fileName, fileHandle) != 0){
		return -1;
	}
	rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
	if(rbfm->closeFile(fileHandle) != 0)
    {
        return -1;
    }
	return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if(isSystemTable(tableName) != 0) return -1;	
	FileHandle filehandle;
	vector<Attribute> descriptor;
    getAttributes(tableName,descriptor);
	if(rbfm->openFile(tableName,filehandle) != 0){
		return -1;
	}
	if(rbfm->deleteRecord(filehandle,descriptor,rid) != 0){
		return -1;
	}
	if(rbfm->closeFile(filehandle) != 0)
    {
        return -1;
    }
	return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if(isSystemTable(tableName) != 0) return -1;	
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();	
	FileHandle fileHandle;
    if(rbfm->openFile(tableName, fileHandle) != 0)
    {
        return -1;
    }
    vector<Attribute> attr;
    if(getAttributes(tableName, attr) != 0)
    {
        return -1;
    }
    if(rbfm->updateRecord(fileHandle, attr, data, rid) != 0)
    {
        return -1;
    }
    if(rbfm->closeFile(fileHandle) != 0)
    {
        return -1;
    }
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();	
	FileHandle fileHandle;
    if(rbfm->openFile(tableName, fileHandle) != 0)
    {
        return -1;
    }
    vector<Attribute> attr;
    if(getAllAttributes(tableName, attr) != 0)
    {
        return -1;
    }
    if(rbfm->readRecord(fileHandle, attr, rid, data) != 0)
    {
        return -1;
	}
	/*
	cout<<"read tuple:"<<endl;
	rbfm->printRecord(attr,data);
	*/
	vector<Attribute> attr_all;
	getAttributes(tableName, attr_all);
	if(attr_all.size() != attr.size()){
		removeNonExisted(tableName, data);		
	}

    if(rbfm->closeFile(fileHandle) != 0)
    {
        return -1;
    }
    return 0;
}

RC RelationManager::removeNonExisted(const string &tableName, void* data)
{
	int tableId;
	getTableId(tableName,tableId);
    vector<Attribute> attr;
    getAllAttributes(tableName, attr);
	int leng = 0, reducedLeng = 0;
	leng = getSizeOfdata(attr, data);
	
	RM_ScanIterator rm_scanIterator;
	vector<string> attrname;
	attrname.push_back("table-id");
	attrname.push_back("column-name");
	attrname.push_back("column-type");
	attrname.push_back("column-length");
	attrname.push_back("column-position");
	attrname.push_back("delete-flag");
	CompOp compOp = EQ_OP;
	
    scan("Columns", "table-id", compOp, &tableId, attrname, rm_scanIterator);
	RID rid;
	void *returnedData=malloc(PAGE_SIZE);

    int totalOffset = ceil((double) attr.size() / CHAR_BIT);
    while(rm_scanIterator.getNextTuple(rid, returnedData) != RM_EOF)
    {
        int columnId, isDeleted;
        AttrType columnType;
		AttrLength columnLength;
		int nFields = attrname.size();
		int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
		int offset = nullFieldsIndicatorActualSize;
		offset+=sizeof(int);
				
		int columnNameLength;		
        memcpy(&columnNameLength, (char *)returnedData + offset, sizeof(int));
        offset += sizeof(int);
		char *columnName_c = (char*) malloc(columnNameLength + 1);
		memcpy(columnName_c, (char *)returnedData + offset, columnNameLength);
		offset += columnNameLength;		
		columnName_c[columnNameLength] = '\0';
		string columnName = string(columnName_c);
		memcpy(&columnType, (char *)returnedData + offset, sizeof(int));
        offset += 3 * sizeof(int);
        memcpy(&isDeleted, (char *)returnedData + offset, sizeof(int));
		offset += sizeof(int);
		
        if(isDeleted != 1)
        {
            if(columnType == TypeVarChar)
            {
                int stringLen = 0;
                memcpy(&stringLen, (char*)data + totalOffset, sizeof(int));
                int stringTotalLength = stringLen + sizeof(int);
                totalOffset += stringTotalLength;
            } else
            {
                totalOffset += sizeof(int);
            }
		} else
        {
            if(columnType == TypeVarChar)
            {
				int stringLen = 0;
                memcpy(&stringLen, (char*)data + totalOffset, sizeof(int));
                int stringTotalLength = stringLen + sizeof(int);
                memcpy((char*)data + totalOffset, (char*)data + totalOffset + stringTotalLength, leng - reducedLeng - totalOffset - stringTotalLength);
                reducedLeng += stringTotalLength;
				memset((char*)data + leng - reducedLeng, 0, stringTotalLength);
                totalOffset += stringTotalLength;
            } else
            {
                memcpy((char*)data + totalOffset, (char*)data + totalOffset + sizeof(int), leng  - reducedLeng - totalOffset - sizeof(int));
                reducedLeng += sizeof(int);
                memset((char*)data + leng - reducedLeng, 0, sizeof(int));
                totalOffset += sizeof(int);
            }
		}
		
	}
	free(returnedData);
    return 0;
}

int RelationManager::getSizeOfdata(vector<Attribute> &attr, void* data)
{
	int nFields = attr.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    int offset = nullFieldsIndicatorActualSize;
    for(int i = 0; i < (int)attr.size(); i++)
    {
        if(attr[i].type == TypeVarChar)
        {
            int stringLen = 0;
            memcpy(&stringLen, (char*)data + offset, sizeof(int));
            offset += stringLen;
            offset += sizeof(int);
        } else
        {
            offset += sizeof(int);
        }
    }
    return offset;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	if(rbfm->printRecord(attrs, data) != 0){
		return -1;
	}
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
    if(rbfm->openFile(tableName, fileHandle) != 0)
    {
        return -1;
    }
    vector<Attribute> attr;
    if(getAttributes(tableName, attr) != 0)
    {
        return -1;
    }
    if(rbfm->readAttribute(fileHandle, attr, rid, attributeName, data) != 0)
    {
        return -1;
    }
    if(rbfm->closeFile(fileHandle) != 0)
    {
        return -1;
    }
    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();	
	string fileName;
	vector <Attribute> recordDescriptor;
	if (tableName == "Tables")
		prepareCatalogTableDescriptor(recordDescriptor);
	else if (tableName == "Columns")
		prepareCatalogColumnDescriptor(recordDescriptor);
	else
		getAttributes(tableName, recordDescriptor);
	if (getFileNameByTableName(tableName, fileName) != 0)
		return -1;
	// printf("%s\n", fileName.c_str());
	if(rbfm->openFile(fileName, rm_ScanIterator.fileHandle) != 0){
		return -1;
	}
	// RBFM_ScanIterator rbfm_ScanIterator;
	if(rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator) != 0){
		return -1;
	}
	return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	vector<Attribute> descriptor;
	RM_ScanIterator rm_ScanIterator;
	int tableId;
	if(this->getTableId(tableName, tableId) != 0)
		return -1;
	vector<string> attrname;
	attrname.push_back("table-id");
	attrname.push_back("column-name");
	attrname.push_back("column-type");
	attrname.push_back("column-length");
	attrname.push_back("column-position");
	attrname.push_back("delete-flag");

	this->scan("Columns", "table-id", EQ_OP, &tableId,attrname,rm_ScanIterator);
	vector<Attribute> columnDescriptor;
	prepareCatalogColumnDescriptor(columnDescriptor);
	
	RID rid;	
	char *data=(char *)malloc(PAGE_SIZE);	
	while(rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
	{
		
		//int nFields = columnDescriptor.size();
		//int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
		int offset = 1;
		int columnNameLength;
		offset += sizeof(int);
		
		memcpy(&columnNameLength, (char*)data + offset, sizeof(int));	
		offset += sizeof(int);
		char *columnName_c = (char*) malloc(columnNameLength + 1);
		memcpy(columnName_c, (char *)data + offset, columnNameLength);
		offset += columnNameLength;	
		
		columnName_c[columnNameLength] = '\0';
		string columnName = string(columnName_c);
		
		if(columnName.compare(attributeName) == 0){
			
			int isDeleted = 1;
			offset = 1 + 2 * sizeof(int) + columnNameLength + 3 * sizeof(int);
			memcpy((char *)data + offset,&isDeleted,sizeof(int));   
			rbfm->openFile("Columns",filehandle);
			vector<Attribute> cAttr;			
			//rbfm->printRecord(columnDescriptor,data);
			rbfm->updateRecord(filehandle,columnDescriptor,data,rid);

		}
		free(columnName_c);
	}
	free(data);
	rbfm->closeFile(filehandle);
	rm_ScanIterator.close();
	
	return 0;
}



// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	
	int tableId;
	if(this->getTableId(tableName, tableId) != 0)
		return -1;
	vector<Attribute> descriptor;	
	getAttributes(tableName,descriptor);

	int position = descriptor.size() + 1;
	
	void *data=malloc(PAGE_SIZE);
	FileHandle table_filehandle;
	RID rid;
	
	vector<Attribute> columndescriptor;
	prepareCatalogColumnDescriptor(columndescriptor);
	if(rbfm->openFile("Columns", table_filehandle) != 0){
		return -1;
	}
	prepareColumnsRecord(columndescriptor,data,tableId,attr,position,0);
	rbfm->insertRecord(table_filehandle,columndescriptor,data,rid);
	rbfm->closeFile(table_filehandle);

	free(data);

	return 0;
}

RC RelationManager::getFileNameByTableName(const string &tableName, string &fileName)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	FileHandle fileHandle;
	if(rbfm->openFile("Tables", fileHandle) != 0)
		return -1;
	vector<Attribute> recordDescriptor;
	prepareCatalogTableDescriptor(recordDescriptor);
	string conditionAttribute = "table-name";
	int nameLength = tableName.length();
	char *value = (char *)malloc(tableName.length() + sizeof(int));
	memcpy(value, &nameLength, sizeof(int));
	memcpy(value + sizeof(int), tableName.c_str(), tableName.length());

	vector<string> attributeNames;
	string attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "table-name";
	attributeNames.push_back(attributeName);
	attributeName = "file-name";
	attributeNames.push_back(attributeName);
	attributeName = "system-table";
	attributeNames.push_back(attributeName);
	CompOp compOp = EQ_OP;
	RBFM_ScanIterator rbfm_ScanIterator;

	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);

	RID rid;
	void *data = malloc(PAGE_SIZE);
	int tableId;

	if(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		int offset = 1;
		// rbfm->printRecord(recordDescriptor, data);
		memcpy(&tableId, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		int nameLength;
		memcpy(&nameLength, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		char *fileName_c = (char*) malloc(nameLength + 1);
		memcpy(fileName_c, (char *)data + offset, nameLength);
		fileName_c[nameLength] = '\0';
		fileName = string(fileName_c);
		// printf("nameLength: %d, fileName: %s, fileName_c: %s, tableId: %d\n", nameLength, fileName.c_str(), fileName_c, tableId);
	}
	else {
		free(data);
		free(value);
		return -1;
	}
	free(data);
	free(value);
	return 0;
}

RC RelationManager::prepareCatalogTableDescriptor(vector<Attribute> &attributes){
	Attribute attr;
	attr.name="table-id";
	attr.type=TypeInt;
	attr.length=4;
	attributes.push_back(attr);

	attr.name="table-name";
	attr.type=TypeVarChar;
	attr.length=50;
	attributes.push_back(attr);

	attr.name="file-name";
	attr.type=TypeVarChar;
	attr.length=50;
	attributes.push_back(attr);

	attr.name="system-table";
	attr.type=TypeInt;
	attr.length=4;
	attributes.push_back(attr);

	return 0;

}
RC RelationManager::prepareCatalogColumnDescriptor(vector<Attribute> &attributes){	
	Attribute attr;	
	attr.name="table-id";
	attr.type=TypeInt;
	attr.length=4;
	attributes.push_back(attr);

	attr.name="column-name";
	attr.type=TypeVarChar;
	attr.length=50;
	attributes.push_back(attr);

	attr.name="column-type";
	attr.type=TypeInt;
	attr.length=4;
	attributes.push_back(attr);

	attr.name="column-length";
	attr.type=TypeInt;
	attr.length=4;
	attributes.push_back(attr);

	attr.name="column-position";
	attr.type=TypeInt;
	attr.length=4;
	attributes.push_back(attr);

	attr.name="delete-flag";
	attr.type=TypeInt;
	attr.length=4;
	attributes.push_back(attr);

	return 0;
}

RC RelationManager::prepareTablesRecord(const vector<Attribute> &recordDescriptor, void *data,int tableId,const string tablename,int isSystemTable){
	int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
	memcpy((char *)data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	int offset = nullFieldsIndicatorActualSize;
	memcpy((char *)data+offset,&tableId,sizeof(int));
	offset = offset+sizeof(int);
	int size = tablename.length();

	memcpy((char *)data+offset,&size,sizeof(int));
	offset = offset+sizeof(int);
	memcpy((char *)data+offset,tablename.c_str(),size);
	offset = offset+size;
	memcpy((char *)data+offset,&size,sizeof(int));
	offset = offset+sizeof(int);
	memcpy((char *)data+offset,tablename.c_str(),size);
	offset = offset+size;
	memcpy((char *)data+offset,&isSystemTable,sizeof(int));
	offset = offset+sizeof(int);

	free(nullFieldsIndicator);
}

RC RelationManager::prepareColumnsRecord(const vector<Attribute> &recordDescriptor, void *data,int tableId,Attribute attr, int position,int deleteFlag){
	int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
	memcpy((char *)data, nullFieldsIndicator, nullFieldsIndicatorActualSize);

	int offset = nullFieldsIndicatorActualSize;
	memcpy((char *)data+offset,&tableId,sizeof(int));
	offset = offset+sizeof(int);
	int size = attr.name.size();
	memcpy((char *)data+offset,&size,sizeof(int));
	offset = offset+sizeof(int);
	memcpy((char *)data+offset,attr.name.c_str(),size);
	offset = offset+size;
	memcpy((char *)data+offset,&(attr.type),sizeof(int));
	offset=offset+sizeof(int);
	memcpy((char *)data+offset,&(attr.length),sizeof(int));
	offset=offset+sizeof(int);
	memcpy((char *)data+offset,&position,sizeof(int));
	offset=offset+sizeof(int);
	memcpy((char *)data+offset,&deleteFlag,sizeof(int));  // 1 : already be deleted  
	offset=offset+sizeof(int);

	free(nullFieldsIndicator);
}

RC RelationManager::insertColumn(int tableId, const vector<Attribute> &attributes){
	void *data=malloc(PAGE_SIZE);
	FileHandle table_filehandle;
	RID rid;
	
	vector<Attribute> columndescriptor;
	prepareCatalogColumnDescriptor(columndescriptor);
	if(rbfm->openFile("Columns", table_filehandle) != 0){
		return -1;
	}
	for(int i=0;i<attributes.size();i++){
		int isDeleted = attributes[i].length == 0 ? 1 : 0;
		prepareColumnsRecord(columndescriptor, data,tableId,attributes[i],i+1,isDeleted);
		rbfm->insertRecord(table_filehandle,columndescriptor,data,rid);
	}
	rbfm->closeFile(table_filehandle);
	free(data);
	return 0;
}

RC RelationManager::getTableId(const string &tableName, int &tableId){
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	FileHandle fileHandle;
	if(rbfm->openFile("Tables", fileHandle) != 0)
		return -1;
	vector<Attribute> recordDescriptor;
	prepareCatalogTableDescriptor(recordDescriptor);
	string conditionAttribute = "table-name";

	int nameLength = tableName.length();
	char *value = (char *)malloc(nameLength + sizeof(int));
	memcpy(value, &nameLength, sizeof(int));
	memcpy(value + sizeof(int), tableName.c_str(), nameLength);

	vector<string> attributeNames;
	string attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "table-name";
	attributeNames.push_back(attributeName);
	attributeName = "file-name";
	attributeNames.push_back(attributeName);
	attributeName = "system-table";
	attributeNames.push_back(attributeName);
	CompOp compOp = EQ_OP;
	RBFM_ScanIterator rbfm_ScanIterator;

	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);

	RID rid;
	void *data = malloc(PAGE_SIZE);
	string fileName;
	// int tableId;

	if(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		int offset = 1;
		// rbfm->printRecord(recordDescriptor, data);
		memcpy(&tableId, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		int nameLength;
		memcpy(&nameLength, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		char *fileName_c = (char*) malloc(nameLength + 1);
		memcpy(fileName_c, (char *)data + offset, nameLength);
		fileName_c[nameLength] = '\0';
		fileName = string(fileName_c);
#ifdef DEBUG
		printf("nameLength: %d, fileName: %s, fileName_c: %s, tableId: %d\n", nameLength, fileName.c_str(), fileName_c, tableId);
#endif
	}
	else return -1;
	rbfm_ScanIterator.close();
	free(value);
	free(data);
	return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
	if(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
    {
        return 0;
    } else
    {
        return RM_EOF;
    }
}

RC RM_ScanIterator::close()
{
    // if(rbfmScanIterator != NULL)
    // {
    //     rbfmScanIterator->close();
    //     // delete rbfmScanIterator;
    //     rbfmScanIterator = 0;
    // }
    rbfm_ScanIterator.close();
    fileHandle.closeFile();
    return 0;
}

int RelationManager::generateNextTableId(){
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	

	void *data = malloc(PAGE_SIZE);
	string conditionAttribute = "table-id";
	CompOp compOp = NO_OP;
	char *value = (char *)malloc(1);
	vector<string> attributeNames;	
	string attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "table-name";
	attributeNames.push_back(attributeName);
	attributeName = "file-name";
	attributeNames.push_back(attributeName);
	attributeName = "system-table";
	attributeNames.push_back(attributeName);
	RM_ScanIterator rm_ScanIterator;
	scan("Tables",conditionAttribute,compOp,value,attributeNames,rm_ScanIterator);

	RID rid;
	string fileName;
	int maxId = 1;
	vector<Attribute> attributes;
	while(rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
	{
		int tableId;
		int offset = ceil((double) attributeNames.size() / CHAR_BIT);
		memcpy(&tableId, (char *)data + offset, sizeof(int));
		if(tableId > maxId) maxId = tableId;
	}
	maxId++;
	rm_ScanIterator.close();	
	free(data);
	return maxId;
}

int RelationManager::isSystemTable(const string &tableName){
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	

	void *data = malloc(PAGE_SIZE);
	string conditionAttribute = "table-name";
	CompOp compOp = EQ_OP;
	int nameLength = tableName.length();
	char *value = (char *)malloc(tableName.length() + sizeof(int));
	memcpy(value, &nameLength, sizeof(int));
	memcpy(value + sizeof(int), tableName.c_str(), tableName.length());
	vector<string> attributeNames;	
	string attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "table-name";
	attributeNames.push_back(attributeName);
	attributeName = "file-name";
	attributeNames.push_back(attributeName);
	attributeName = "system-table";
	attributeNames.push_back(attributeName);
	RM_ScanIterator rm_ScanIterator;
	this->scan("Tables",conditionAttribute,compOp,value,attributeNames,rm_ScanIterator);
	int systemTableFlag;
	RID rid;
	string fileName;
	vector<Attribute> attributes;
	while(rm_ScanIterator.getNextTuple(rid, data) != RM_EOF)
	{
		int offset = ceil((double) attributeNames.size() / CHAR_BIT);
		offset += sizeof(int);
		int nameLength;
		memcpy(&nameLength, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		offset += nameLength;
		int fileNameLength;
		memcpy(&fileNameLength, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		offset += fileNameLength;
		memcpy(&systemTableFlag, (char *)data + offset, sizeof(int));
	}
	rm_ScanIterator.close();	
	free(data);
	free(value);
	return systemTableFlag;
}
