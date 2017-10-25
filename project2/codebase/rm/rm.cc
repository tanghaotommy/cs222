
#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{

}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    vector<Attribute> tablesdescriptor;
	vector<Attribute> columndescriptor;

	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle table_filehandle;
	RID rid;

	if((rbfm->createFile("Tables"))!=0){
		return -1;
	}
	int tableId = 1;
	rbfm->openFile("Tables",table_filehandle);
	void *data = malloc(PAGE_SIZE);
	prepareCatalogTableDescriptor(tablesdescriptor);

	prepareTablesRecord(tablesdescriptor, data,"Tables",1);
	rbfm->printRecord(tablesdescriptor, data);
	int nFields = tablesdescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memcpy(&nullFieldsIndicator,(char *)data,nullFieldsIndicatorActualSize);
	RC rc = rbfm->insertRecord(table_filehandle,tablesdescriptor,data,rid);
	if(rc != 0) return -1;
	free(data);

	tableId = 2;
	void *data2 = malloc(PAGE_SIZE);
	prepareTablesRecord(tablesdescriptor,data2,"Columns",1);
	nFields = tablesdescriptor.size();
    nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	memcpy(&nullFieldsIndicator,(char *)data2,nullFieldsIndicatorActualSize);
	rbfm->printRecord(tablesdescriptor, data2);
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
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
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
	
	RID rid;
	prepareCatalogTableDescriptor(tablesdescriptor);

	prepareTablesRecord(tablesdescriptor,data,tableName,0);
	
	rbfm->printRecord(tablesdescriptor, data);
	RC rc = rbfm->insertRecord(filehandle,tablesdescriptor,data,rid);
	free(data);
	if(rc != 0) return -1;
	rbfm->closeFile(filehandle);
	int tableId = getTableId(tablesdescriptor);
	rc = insertColumn(tableId, attrs);

	if (rc != 0) return -1;
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	FileHandle fileHandle;
	if(rbfm->openFile("Tables", fileHandle) != 0)
		return -1;
	vector<Attribute> recordDescriptor;
	prepareCatalogTableDescriptor(recordDescriptor);
	string conditionAttribute = "table-name";
	char *value = (char *)malloc(tableName.length() + 1);
	memcpy(value, tableName.c_str(), tableName.length());
	value[tableName.length()] = '\0';
	vector<string> attributeNames;
	string attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "table-name";
	attributeNames.push_back(attributeName);
	attributeName = "file-name";
	attributeNames.push_back(attributeName);
	attributeName="idCounter";
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
		//rbfm->printRecord(recordDescriptor, data);
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

	if(rbfm->destroyFile(fileName) != 0)
		return -1;

	conditionAttribute = "table-id";
	realloc(value, sizeof(int));
	memcpy(value, &tableId, sizeof(int));
	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
	if(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		rbfm->printRecord(recordDescriptor, data);
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

	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
	while(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		rbfm->printRecord(recordDescriptor, data);
		rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
	}
	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	int tableId;
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();		
	tableId = this->getCurrentTableId(tableName);
	//printf("TableId: %d\n", tableId);
	if(tableId < 0)
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
		//rbfm->printRecord(recordDescriptor, data);
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
#ifdef DEBUG
		printf("nameLength: %d, fileName: %s, fileName_c: %s, tableId: %d, type: %d, column-length:%d, column-position: %d\n", 
			nameLength, fileName.c_str(), fileName_c, tableId, columnType, columnLength, columnPosition);
#endif
		attr.name = fileName;
		attr.type = columnType;
		attr.length = columnLength;
		vect.push_back(make_pair(columnPosition, attr));
		// attrs.push_back(attr);
	}
	rm_ScanIterator.close();

	sort(vect.begin(), vect.end(), sortByPosition);
	//printf("%s\n", vect[0].second.name.c_str());
	for (int i = 0; i < vect.size(); ++i)
	{
		attrs.push_back(vect[i].second);
	}
	// else return -1;
	
	return 0;
	/*
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	vector<Attribute> recordDescriptor;
	prepareCatalogTableDescriptor(recordDescriptor);
	string conditionAttribute = "table-name";
	char *value = (char *)malloc(tableName.length() + 1);
	memcpy(value, tableName.c_str(), tableName.length());
	value[tableName.length()] = '\0';
	vector<string> attributeNames;
	string attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "table-name";
	attributeNames.push_back(attributeName);
	attributeName = "file-name";
	attributeNames.push_back(attributeName);
	attributeName="idCounter";
	attributeNames.push_back(attributeName);
	attributeName = "system-table";
	attributeNames.push_back(attributeName);

	CompOp compOp = EQ_OP;
	RBFM_ScanIterator rbfm_ScanIterator;

	FileHandle fileHandle;
	rbfm->openFile("Tables", fileHandle);

	RID rid;
	void *data = malloc(PAGE_SIZE);
	string fileName;
	int tableId;

	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
	if(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		int offset = 1;
		memcpy(&tableId, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
	}
	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);
	
	vector<string> attrname;
	attrname.push_back("table-id");
	attrname.push_back("column-name");
	attrname.push_back("column-type");
	attrname.push_back("column-length");
	attrname.push_back("column-position");
	attrname.push_back("system-table");
	
	Attribute attr;
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
	attributeName = "column-position";
	attributeNames.push_back(attributeName);
	attributeName = "column-position";
	attributeNames.push_back(attributeName);
	attributeName = "system-table";
	attributeNames.push_back(attributeName);
	
	conditionAttribute = "table-id";
	realloc(value, sizeof(int));
	memcpy(value, &tableId, sizeof(int));
	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
	
	while(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		
		int offset = 1;
		
		offset += sizeof(int);
		int length;
		memcpy(&length, data + offset, sizeof(int));
		offset += sizeof(int);
		char columnNameCharArray[length + 1];
        memcpy(&columnNameCharArray,data + offset, length);
        columnNameCharArray[length] = '\0';
        string columnName(columnNameCharArray);
        offset += length;
		memcpy(&(attr.type),data + offset,sizeof(int));
		offset+=sizeof(int);
		memcpy(&(attr.length),data + offset,sizeof(int));
		offset+=sizeof(int);
		attr.name = columnName;
		attrs.push_back(attr);
	}

	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);

	free(data);
	*/
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	FileHandle filehandle;
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	vector<Attribute> descriptor;
	
	getAttributes(tableName,descriptor);
	
	if(rbfm->openFile(tableName,filehandle) != 0){
		return -1;
	}
	rbfm->insertRecord(filehandle,descriptor,data,rid);
	if(rbfm->closeFile(filehandle) != 0)
	{
		return -1;
	}
	return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	FileHandle filehandle;
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
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
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	
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
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	if(rbfm->openFile(tableName, fileHandle) != 0)
	{
		return -1;
	}
	vector<Attribute> attr;
	if(getAttributes(tableName, attr) != 0)
	{
		return -1;
	}
	cout<<"table_name:"<<tableName<<" RID pagenum:"<<rid.pageNum<<" RID slot:"<<rid.slotNum<<endl;
	
	cout<<attr[0].name<<" "<<attr[1].name<<" "<<attr[2].name<<" "<<attr[3].name<<endl;
	if(rbfm->readRecord(fileHandle, attr, rid, data) != 0)
	{
		return -1;
	}
	if(rbfm->closeFile(fileHandle) != 0)
	{
	return -1;
	}
	
	return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();		
	if(rbfm->printRecord(attrs,data)!=0){
		return -1;
	}
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();		
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

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
	if(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
    {
        return 0;
    } else
    {
        return RM_EOF;
    }
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
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

	attr.name="idCounter";
	attr.type=TypeInt;
	attr.length=4;
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

	attr.name="system-table";
	attr.type=TypeInt;
	attr.length=4;
	attributes.push_back(attr);

	return 0;
}

RC RelationManager::prepareTablesRecord(const vector<Attribute> &recordDescriptor, void *data,const string tablename,int systemtable){
	int idCounter;
	if(tablename.compare("Tables") != 0){
		idCounter = writeTableId(recordDescriptor,tablename);
	}
	else idCounter = 1;
	int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
	memcpy((char *)data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	int offset = nullFieldsIndicatorActualSize;
	memcpy((char *)data+offset,&idCounter,sizeof(int));
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

	memcpy((char *)data+offset,&idCounter,sizeof(int));
	offset = offset + sizeof(int);	//idCounter

	memcpy((char *)data+offset,&systemtable,sizeof(int));
	offset = offset + sizeof(int);	//systemtable 1:yes 0:no
}

RC RelationManager::prepareColumnsRecord(const vector<Attribute> &recordDescriptor, void *data,int tableId,Attribute attr, int position,int systemtable){
	int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
	memcpy((char *)data, nullFieldsIndicator, nullFieldsIndicatorActualSize);

	int offset = nullFieldsIndicatorActualSize;
	memcpy((char *)data+offset,&tableId,sizeof(int));
	offset = offset + sizeof(int);
	int size = attr.name.length();
	memcpy((char *)data+offset,&size,sizeof(int));
	offset = offset+sizeof(int);
	memcpy((char *)data+offset,attr.name.c_str(),size);
	offset = offset+size;
	memcpy((char *)data+offset,&(attr.type),sizeof(int));
	offset = offset + sizeof(int);
	memcpy((char *)data+offset,&(attr.length),sizeof(int));
	offset = offset+sizeof(int);
	memcpy((char *)data+offset,&position,sizeof(int));
	offset = offset + sizeof(int);  
	memcpy((char *)data+offset,&systemtable,sizeof(int));
	offset = offset + sizeof(int);  

}

RC RelationManager::insertColumn(int tableId, const vector<Attribute> &attributes){
	void *data=malloc(PAGE_SIZE);
	FileHandle table_filehandle;
	RID rid;
	
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	vector<Attribute> columndescriptor;
	prepareCatalogColumnDescriptor(columndescriptor);
	if(rbfm->openFile("Columns", table_filehandle) != 0){
		return -1;
	}
	for(int i=0;i<attributes.size();i++){
		prepareColumnsRecord(columndescriptor, data,tableId,attributes[i],i+1,1);
		rbfm->printRecord(columndescriptor, data);		
		rbfm->insertRecord(table_filehandle,columndescriptor,data,rid);
	}
	rbfm->closeFile(table_filehandle);
	free(data);
	return 0;
}

int RelationManager::getCurrentTableId(const string &tableName){
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	vector<Attribute> recordDescriptor;
	prepareCatalogTableDescriptor(recordDescriptor);
	string conditionAttribute = "table-name";
	char *value = (char *)malloc(tableName.length() + 1);
	memcpy(value, tableName.c_str(), tableName.length());
	value[tableName.length()] = '\0';
	vector<string> attributeNames;
	string attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "table-name";
	attributeNames.push_back(attributeName);
	attributeName = "file-name";
	attributeNames.push_back(attributeName);
	attributeName="idCounter";
	attributeNames.push_back(attributeName);
	attributeName = "system-table";
	attributeNames.push_back(attributeName);

	CompOp compOp = EQ_OP;
	RBFM_ScanIterator rbfm_ScanIterator;

	FileHandle fileHandle;
	rbfm->openFile("Tables", fileHandle);
	int tableId;
	RID rid;
	void *data = malloc(PAGE_SIZE);
	string fileName;
	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
	if(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		int offset = 1;
		memcpy(&tableId, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
	}
	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);
	
	return tableId;
}

int RelationManager::getTableId(vector<Attribute> recordDescriptor){
	FileHandle filehandle;
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();		
	void *data = malloc(PAGE_SIZE);
	rbfm->openFile("Tables", filehandle);
	int nFields = recordDescriptor.size();
	int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	int offset = nullFieldsIndicatorActualSize;
	offset = offset + sizeof(int);
	string s = "Tables";
	offset = offset + sizeof(int);
	offset = offset + s.length();
	offset = offset + sizeof(int);
	offset = offset + s.length();
	int pageNumer = 0;
	filehandle.readPage(pageNumer, data);
	int tableid;
	memcpy(&tableid,(char *)data + offset,sizeof(int));
	rbfm->closeFile(filehandle);
	return tableid;
}

int RelationManager::writeTableId(const vector<Attribute> &recordDescriptor,const string tablename){
		FileHandle filehandle;
		RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();			
		void *data = malloc(PAGE_SIZE);
		rbfm->openFile("Tables", filehandle);
		int nFields = recordDescriptor.size();
		int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
		int offset = nullFieldsIndicatorActualSize;
		offset = offset + sizeof(int);
		string s = "Tables";
		offset = offset + sizeof(int);
		offset = offset + s.length();
		offset = offset + sizeof(int);
		offset = offset + s.length();
		int pageNumer = 0;
		filehandle.readPage(pageNumer, data);
		int idCounter;
		if(tablename.compare("Tables") == 0) idCounter = 1;
		else if(tablename.compare("Columns") == 0) idCounter = 2;
		else{
			memcpy(&idCounter,(char *)data + offset,sizeof(int));
			
			idCounter++;
		}
		
		memcpy((char *)data + offset,&idCounter,sizeof(int));
		filehandle.writePage(0,data);
		rbfm->closeFile(filehandle);
		return idCounter;
}



RC RM_ScanIterator::close()
{	
	rbfm_ScanIterator.close();
	fileHandle.closeFile();
	return 0;
}
/*
RC RM_ScanIterator::setRBFMSI(RBFM_ScanIterator* r)
{
    rbfm_ScanIterator = r;
    return 0;
}
*/
bool RelationManager::sortByPosition(const pair<int, Attribute> &a,
	const pair<int, Attribute> &b)
{
return (a.first < b.first);
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
	char *value = (char *)malloc(tableName.length() + 1);
	memcpy(value, tableName.c_str(), tableName.length());
	value[tableName.length()] = '\0';
	vector<string> attributeNames;
	string attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "table-name";
	attributeNames.push_back(attributeName);
	attributeName = "file-name";
	attributeNames.push_back(attributeName);
	attributeName="idCounter";
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
		//rbfm->printRecord(recordDescriptor, data);
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
	else 
		return -1;
	return 0;
}


int RelationManager::IsSystemTable(const string &tableName){
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	vector<Attribute> recordDescriptor;
	prepareCatalogTableDescriptor(recordDescriptor);
	string conditionAttribute = "table-name";
	char *value = (char *)malloc(tableName.length() + 1);
	memcpy(value, tableName.c_str(), tableName.length());
	value[tableName.length()] = '\0';
	vector<string> attributeNames;
	string attributeName = "table-id";
	attributeNames.push_back(attributeName);
	attributeName = "table-name";
	attributeNames.push_back(attributeName);
	attributeName = "file-name";
	attributeNames.push_back(attributeName);
	attributeName="idCounter";
	attributeNames.push_back(attributeName);
	attributeName = "system-table";
	attributeNames.push_back(attributeName);

	CompOp compOp = EQ_OP;
	RBFM_ScanIterator rbfm_ScanIterator;

	FileHandle fileHandle;
	rbfm->openFile("Tables", fileHandle);

	RID rid;
	void *data = malloc(PAGE_SIZE);
	string fileName;
	int tableId;

	rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
	if(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF)
	{
		int nFields = recordDescriptor.size();
		int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
		int offset = nullFieldsIndicatorActualSize;
		memcpy(&tableId, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		offset = offset + sizeof(int);
		string s = "Tables";
		offset = offset + sizeof(int);
		offset = offset + s.length();
		offset = offset + sizeof(int);
		offset = offset + s.length();
		offset += sizeof(int);		

		int pageNumer = 0;
		fileHandle.readPage(pageNumer, data);
		int issystemtable;
		memcpy(&issystemtable,(char *)data + offset,sizeof(int));

	}
	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);
}
