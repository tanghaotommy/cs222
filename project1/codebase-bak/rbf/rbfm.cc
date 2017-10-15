#include "rbfm.h"
#include <math.h>
#include <stdlib.h> 

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance()->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    // printf("Number of fields: %d, The data is: %s\n", nFields, data);
    void *page = malloc(PAGE_SIZE);
    int cPage = fileHandle.getNumberOfPages() - 1;
    fileHandle.readPage(cPage, page);

    // //length + data
    // int offset = 0;

    // //Get next available position in current page
    // while ( cPage >= 0 && offset < PAGE_SIZE)
    // {
    //     int recordLength;
    //     memcpy(&recordLength, (char *)page + offset, sizeof(int));
    //     // printf("offset pos: %d, recordLength: %d\n", offset, recordLength);
    //     if (recordLength != 0) 
    //     {
    //         offset += sizeof(int);
    //         offset += recordLength;
    //     }
    //     else
    //         break;
    // }

    // int recordLength = this->getRecordLength(recordDescriptor, data);

    // //If there is nothing in the file or not enough space
    // if ( cPage < 0 || PAGE_SIZE - 1 - offset - sizeof(int) < recordLength)
    // {
    //     offset = 0;
    //     cPage++;
    //     for (int i = 0; i < PAGE_SIZE; ++i)
    //     {
    //         ((char *)page)[i] = 0;
    //     }
    //     memcpy((char *)page, &recordLength, sizeof(int));
    //     memcpy((char *)page + sizeof(int), data, recordLength);
    //     fileHandle.appendPage(page);
    // }
    // else
    // {
    //     memcpy((char *)page + offset, &recordLength, sizeof(int));
    //     memcpy((char *)page + offset + sizeof(int), data, recordLength);
    //     fileHandle.writePage(cPage, page);
    // }
    // rid.pageNum = cPage;
    // rid.slotNum = offset;
    // delete page;
    // return 0;

    //total + (length, data)
    // int total;
    // memcpy(&total, (char *)page, sizeof(int));   
    // int recordLength = this->getRecordLength(recordDescriptor, data);
    // if ( total == 0 || PAGE_SIZE - 1 - total - sizeof(int) < recordLength) {
    //     cPage++;
    //     for (int i = 0; i < PAGE_SIZE; ++i)
    //     {
    //         ((char *)page)[i] = 0;
    //     }
    //     total = 0;
    //     total += sizeof(int);
    //     rid.slotNum = sizeof(int);
    //     total += sizeof(int) + recordLength;
    //     memcpy((char *)page, &total, sizeof(int));
    //     memcpy((char *)page + sizeof(int), &recordLength, sizeof(int));
    //     memcpy((char *)page + sizeof(int) + sizeof(int), data, recordLength);
    //     fileHandle.appendPage(page);       
    // }
    // else
    // {
    //     rid.slotNum = total;
    //     memcpy((char *)page + total, &recordLength, sizeof(int));
    //     memcpy((char *)page + total + sizeof(int), data, recordLength);

    //     total += sizeof(int) + recordLength;
    //     memcpy((char *)page, &total, sizeof(int));
    //     fileHandle.writePage(cPage, page);        
    // }
    // rid.pageNum = cPage;
    // // printf("pageNum: %d, slotNum: %d, length: %d\n", rid.pageNum, rid.slotNum, recordLength);
    // delete page;
    // return 0;    

    int total;
    int *indexList = new int[nFields];
    this->getIndexList(recordDescriptor, data, indexList);

    memcpy(&total, (char *)page, sizeof(int));   
    int recordLength = this->getRecordLength(recordDescriptor, data);
    int left = PAGE_SIZE - 1 - total - sizeof(int) - nFields * sizeof(int);
    
    if ( total == 0 || left < recordLength)
    {
        cPage++;
        for (int i = 0; i < PAGE_SIZE; ++i)
        {
            ((char *)page)[i] = 0;
        }
        total = 0;
        total += sizeof(int);

        for (int i = 0; i < nFields; ++i)
        {
            memcpy((char *)page + total + i * sizeof(int), &indexList[i], sizeof(int));
        }
        total += nFields * sizeof(int);
        rid.slotNum = sizeof(int);
        total += sizeof(int) + recordLength;
        memcpy((char *)page, &total, sizeof(int));
        memcpy((char *)page + (nFields + 1) * sizeof(int), &recordLength, sizeof(int));
        memcpy((char *)page + (nFields + 2) * sizeof(int), data, recordLength);
        fileHandle.appendPage(page);       
    }
    else
    {
        rid.slotNum = total;
        for (int i = 0; i < nFields; ++i)
        {
            memcpy((char *)page + total + i * sizeof(int), &indexList[i], sizeof(int));
        }
        total += nFields * sizeof(int);
        memcpy((char *)page + total, &recordLength, sizeof(int));
        memcpy((char *)page + total + sizeof(int), data, recordLength);

        total += sizeof(int) + recordLength;
        memcpy((char *)page, &total, sizeof(int));
        fileHandle.writePage(cPage, page);        
    }
    rid.pageNum = cPage;
    // printf("pageNum: %d, slotNum: %d, length: %d\n", rid.pageNum, rid.slotNum, recordLength);
    delete page;
    return 0;    
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    int offset = rid.slotNum;
    int cPage = rid.pageNum;
    int nFields = recordDescriptor.size();
    int recordLength;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(cPage, page);
    memcpy(&recordLength, (char *)page + offset + (nFields) * sizeof(int), sizeof(int));
    memcpy(data, (char *)page + offset + (nFields + 1) * sizeof(int), recordLength);
    // printf("record length: %d\n", recordLength);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // printf("Size of data: %d, data: %c\n", sizeof(data), ((char *)data)[0]);
    int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
    {
        nullFieldsIndicator[i] = ((char *)data)[i];
    }
    int offset = nullFieldsIndicatorActualSize;
    // printf("Number of fields: %d, actual field size: %d, %d\n", nFields, nullFieldsIndicatorActualSize, nullFieldsIndicator[0]);
    for (int i = 0; i < nFields; ++i)
    {
        int nByte = i / 8;
        int nBit = i % 8;
        bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
        if(!nullBit)
        {
            if (recordDescriptor[i].type == TypeInt)
            {
                int value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
                printf("%s: %-10d\t", recordDescriptor[i].name.c_str(), value);
            }
            if (recordDescriptor[i].type == TypeReal)
            {
                float value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
                printf("%s: %-10f\t", recordDescriptor[i].name.c_str(), value);
            }
            if (recordDescriptor[i].type == TypeVarChar)
            {
                int nameLength;
                memcpy(&nameLength, (char *)data + offset, sizeof(int));
                offset += sizeof(int);
                // printf("String length: %d\n", nameLength);
                char* value = (char *) malloc(nameLength);
                memcpy(value, (char *)data + offset, nameLength);
                offset += nameLength;
                printf("%s: %-10s\t", recordDescriptor[i].name.c_str(), value);
            }          
        }
        else
        {
            printf("%s: NULL\t", recordDescriptor[i].name.c_str());
        }
    }
    printf("\n");
    return 0;
}

RC RecordBasedFileManager::getRecordLength(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // printf("Size of data: %d, data: %c\n", sizeof(data), ((char *)data)[0]);
    int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
    {
        nullFieldsIndicator[i] = ((char *)data)[i];
    }
    int offset = nullFieldsIndicatorActualSize;
    // printf("Number of fields: %d, actual field size: %d, %d\n", nFields, nullFieldsIndicatorActualSize, nullFieldsIndicator[0]);
    for (int i = 0; i < nFields; ++i)
    {
        int nByte = i / 8;
        int nBit = i % 8;
        bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
        if(!nullBit)
        {
            if (recordDescriptor[i].type == TypeInt)
            {
                int value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
            }
            if (recordDescriptor[i].type == TypeReal)
            {
                float value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
            }
            if (recordDescriptor[i].type == TypeVarChar)
            {
                int nameLength;
                memcpy(&nameLength, (char *)data + offset, sizeof(int));
                offset += sizeof(int);
                // printf("String length: %d\n", nameLength);
                char* value = (char *) malloc(nameLength);
                memcpy(value, (char *)data + offset, nameLength);
                offset += nameLength;
            }          
        }
    }
    return offset;
}

RC RecordBasedFileManager::getIndexList(const vector<Attribute> &recordDescriptor, const void *data, int *indexList)
{
    int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
    {
        nullFieldsIndicator[i] = ((char *)data)[i];
    }
    int offset = nullFieldsIndicatorActualSize;
    // printf("Number of fields: %d, actual field size: %d, %d\n", nFields, nullFieldsIndicatorActualSize, nullFieldsIndicator[0]);
    for (int i = 0; i < nFields; ++i)
    {
        int nByte = i / 8;
        int nBit = i % 8;
        bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
        if(!nullBit)
        {
            indexList[i] = offset;
            if (recordDescriptor[i].type == TypeInt)
            {
                int value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
            }
            if (recordDescriptor[i].type == TypeReal)
            {
                float value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
            }
            if (recordDescriptor[i].type == TypeVarChar)
            {
                int nameLength;
                memcpy(&nameLength, (char *)data + offset, sizeof(int));
                offset += sizeof(int);
                // printf("String length: %d\n", nameLength);
                char* value = (char *) malloc(nameLength);
                memcpy(value, (char *)data + offset, nameLength);
                offset += nameLength;
            }          
        }
        else
        {
            indexList[i] = -1;
        }
    }
    return 0;
}
