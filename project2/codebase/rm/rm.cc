
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

	prepareTablesRecord(tablesdescriptor, data,tableId,"Tables");
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
	prepareTablesRecord(tablesdescriptor,data2,tableId,"Columns");
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
	int tableId = getTableId();
	RID rid;
	prepareCatalogTableDescriptor(tablesdescriptor);
	prepareTablesRecord(tablesdescriptor, data,tableId,tableName);
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
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	FileHandle fileHandle;
	rbfm->openFile("Tables", fileHandle);
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
		rbfm->printRecord(recordDescriptor, data);
		memcpy(&tableId, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		int nameLength;
		memcpy(&nameLength, (char *)data + offset, sizeof(int));
		offset += sizeof(int);
		char *fileName_c = (char*) malloc(nameLength + 1);
		memcpy(fileName_c, (char *)data + offset, nameLength);
		fileName_c[nameLength] = '\0';
		fileName = string(fileName_c);
		printf("nameLength: %d, fileName: %s, fileName_c: %s, tableId: %d\n", nameLength, fileName.c_str(), fileName_c, tableId);
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
		printf("slotNum: %d, pageNum: %d\n", rid.slotNum, rid.pageNum);
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
	//TODO
	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	RM_ScanIterator rm_ScanIterator;
	RID rid;
	int tableid;
	char *data=(char *)malloc(PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("column-name");
	attrname.push_back("column-type");
	attrname.push_back("column-length");
	attrname.push_back("column-position");
	Attribute attr;
	string tempstr;
	
	
	tableid=getTableId();  
	if( tableid == -1 ) return -1;

	if(scan("Columns","table-id",EQ_OP,&tableid,attrname,rm_ScanIterator) != 0 ){
		return -1;
	}
	while(rm_ScanIterator.getNextTuple(rid,data) != RM_EOF){
		int offset = 1;
		memcpy(&columnNameLength, data + offset, sizeof(int));
		offset += sizeof(int);
		char columnNameCharArray[columnNameLength + 1];
        memcpy(&columnNameCharArray,data + offset, columnNameLength);
        columnNameCharArray[columnNameLength] = '\0';
        string columnName(columnNameCharArray);
        offset += columnNameLength;
		memcpy(&(attr.type),data + offset,sizeof(int));
		offset+=sizeof(int);
		memcpy(&(attr.length),data + offset,sizeof(int));
		offset+=sizeof(int);
		memcpy(&(attr.position),data + offset,sizeof(int));
		offset+=sizeof(int);
		attrs.push_back(attr);
	}
	rm_ScanIterator.close();
	free(data);
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	FileHandle filehandle;
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	vector<Attribute> descriptor;
	if(IsSystemTable(tableName) != 0){
		return -1;
	}
	getAttributes(tableName,descriptor);
	if(rbfm->openFile(tableName,filehandle) != 0){
		return -1;
	}
	rbfm->insertRecord(filehandle,descriptor,data,rid);
	if(_rbfm->closeFile(fileHandle) != 0)
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
	if(_rbfm->closeFile(fileHandle) != 0)
    {
        return -1;
    }
	return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    FileHandle fileHandle;
    if(_rbfm->openFile(tableName, fileHandle) != 0)
    {
        return -1;
    }
    vector<Attribute> attr;
    if(getAttributes(tableName, attr) != 0)
    {
        return -1;
    }
    if(_rbfm->updateRecord(fileHandle, attr, data, rid) != 0)
    {
        return -1;
    }
    if(_rbfm->closeFile(fileHandle) != 0)
    {
        return -1;
    }
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    FileHandle fileHandle;
    if(_rbfm->openFile(tableName, fileHandle) != 0)
    {
        return -1;
    }
    vector<Attribute> attr;
    if(getAttributes(tableName, attr) != 0)
    {
        return -1;
    }
    if(_rbfm->readRecord(fileHandle, attr, rid, data) != 0)
    {
        return -1;
    }
    if(removeNonExisted(tableName, data) != 0)
    {
        return -1;
    }
    if(_rbfm->closeFile(fileHandle) != 0)
    {
        return -1;
    }
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	if(rbfm->printRecord(attrs,data)!=0){
		return -1;
	}
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
    if(_rbfm->openFile(tableName, fileHandle) != 0)
    {
        return -1;
    }
    vector<Attribute> attr;
    if(getAttributes(tableName, attr) != 0)
    {
        return -1;
    }
    if(_rbfm->readAttribute(fileHandle, attr, rid, attributeName, data) != 0)
    {
        return -1;
    }
    if(_rbfm->closeFile(fileHandle) != 0)
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
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();	
	FileHandle filehandle;
	vector<Attribute> descriptor;
	if(tableName.compare("Tables")==0){
		prepareCatalogTableDescriptor(descriptor);
	}
	else if(tableName.compare("Columns")==0){
		prepareCatalogColumnDescriptor(descriptor);
	}
	else{
		getAttributes(tableName,descriptor);
	}
	if(rbfm->openFile(tableName,filehandle)!=0){
		return -1;
	}
	RBFM_ScanIterator* rbfmScanIterator = new RBFM_ScanIterator();
	if(rbfm->scan(filehandle,descriptor,conditionAttribute,compOp,value,attributeNames,*rbfmScanIterator)!=0){
		return -1;
	}
	return 0;

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

	return 0;
}

RC RelationManager::prepareTablesRecord(const vector<Attribute> &recordDescriptor, void *data,int tableId,const string tablename){
	int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
	memcpy((char *)data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	cout<<";;;;"<<(int)*nullFieldsIndicator<<" "<<nullFieldsIndicatorActualSize<<endl;
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
}

RC RelationManager::prepareColumnsRecord(const vector<Attribute> &recordDescriptor, void *data,int tableId,Attribute attr, int position){
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
		prepareColumnsRecord(columndescriptor, data,tableId,attributes[i],i+1);
		rbfm->insertRecord(table_filehandle,columndescriptor,data,rid);
	}
	rbfm->closeFile(table_filehandle);
	free(data);
	return 0;
}

RC RelationManager::getTableId(){
	return 3;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
	if(rbfmScanIterator->getNextRecord(rid, data) != RBFM_EOF)
    {
        return 0;
    } else
    {
        return RM_EOF;
    }
}