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
    // int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
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
    int left = PAGE_SIZE - 1 - total - sizeof(int) - nFields * sizeof(int) - sizeof(int);

    if (left < recordLength)
    {
        for (cPage = 0; cPage < fileHandle.getNumberOfPages(); ++cPage)
        {
            fileHandle.readPage(cPage, page);
            memcpy(&total, (char *)page, sizeof(int));
            left = PAGE_SIZE - 1 - total - sizeof(int) - nFields * sizeof(int) - sizeof(int);
            if (left >= recordLength)
                break;
        }
        if (cPage == fileHandle.getNumberOfPages())
        {
            total = 0;
            cPage = fileHandle.getNumberOfPages() - 1;
        }
    }

    if ( total == 0)
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
            memcpy((char *)page + total + (i + 1) * sizeof(int), &indexList[i], sizeof(int));
        }
        // total += nFields * sizeof(int);
        rid.slotNum = 0;
        recordLength += (nFields + 1) * sizeof(int);
        total += sizeof(int) + recordLength;
        // total += sizeof(int) * 2;
        memcpy((char *)page, &total, sizeof(int));
        memcpy((char *)page + sizeof(int), &recordLength, sizeof(int));
        memcpy((char *)page + (nFields + 2) * sizeof(int), data, recordLength);

        //Slot table
        int size = 1;
        memcpy((char *)page + PAGE_SIZE - sizeof(int), &size, sizeof(int));
        size = sizeof(int);
        memcpy((char *)page + PAGE_SIZE - 2 * sizeof(int), &size, sizeof(int));
        fileHandle.appendPage(page);       
    }
    else
    {
        int nSlot;
        //Get total number of slots
        memcpy(&nSlot, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
        int offset;
        memcpy(&offset, (char *)page + PAGE_SIZE - sizeof(int) - nSlot * sizeof(int), sizeof(int));
        int lastLength;
        memcpy(&lastLength, (char *)page + offset, sizeof(int));
        offset += lastLength;

        rid.slotNum = nSlot;
        for (int i = 0; i < nFields; ++i)
        {
            memcpy((char *)page + offset + (i + 1) * sizeof(int), &indexList[i], sizeof(int));
        }
        // total += nFields * sizeof(int);
        recordLength += (nFields + 1) * sizeof(int);
        memcpy((char *)page + offset, &recordLength, sizeof(int));
        memcpy((char *)page + offset + (nFields + 1) * sizeof(int), data, recordLength - (nFields + 1) * sizeof(int));

        total += sizeof(int) + recordLength;
        memcpy((char *)page, &total, sizeof(int));

        //Slot table
        int size = nSlot + 1;
        memcpy((char *)page + PAGE_SIZE - sizeof(int), &size, sizeof(int));
        memcpy((char *)page + PAGE_SIZE - (nSlot + 2) * sizeof(int), &offset, sizeof(int));
        fileHandle.writePage(cPage, page);        
    }
    rid.pageNum = cPage;


    int offset;
    memcpy(&offset, (char *)page + PAGE_SIZE - (rid.slotNum + 2) * sizeof(int), sizeof(int));
#ifdef DEBUG
    printf("pageNum: %d, slotNum: %d, length: %d, offset: %d\n", rid.pageNum, rid.slotNum, recordLength, offset);
#endif
    free(page);
    return 0;    
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    int cPage = rid.pageNum;
    int nFields = recordDescriptor.size();
    int recordLength;
    int slotNum = rid.slotNum;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(cPage, page);

    int offset;
    memcpy(&offset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
    if (offset == -1)
        return -1;

    memcpy(&recordLength, (char *)page + offset, sizeof(int));

    while (recordLength == -1)
    {
        memcpy(&cPage, (char *)page + offset + sizeof(int), sizeof(int));
        memcpy(&slotNum, (char *)page + offset + 2 * sizeof(int), sizeof(int));
        fileHandle.readPage(cPage, page);
        int offset;
        memcpy(&offset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
        memcpy(&recordLength, (char *)page + offset, sizeof(int));
    }
    memcpy(data, (char *)page + offset + (nFields + 1) * sizeof(int), recordLength - (nFields + 1) * sizeof(int));
    // printf("record length: %d\n", recordLength);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // printf("Size of data: %d, data: %c\n", sizeof(data), ((char *)data)[0]);
    int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
// #ifdef DEBUG
//     for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
//     {
//         printf("%d ", nullFieldsIndicator[i]);
//     }
//     printf("\n");
// #endif
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
                char* value = (char *) malloc(nameLength + 1);
                memcpy(value, (char *)data + offset, nameLength);
                value[nameLength] = '\0';
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
    // for (int i = 0; i < nFields; ++i)
    // {
    //     printf("%d ", indexList[i]);
    // }
    // printf("\n");
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    int cPage = rid.pageNum;
    int nFields = recordDescriptor.size();
    int recordLength;
    int nSlot;
    int slotNum = rid.slotNum;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(cPage, page);
    int total;
    memcpy(&total, (char *)page, sizeof(int));   

    int offset;
    memcpy(&offset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
    memcpy(&nSlot, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
    memcpy(&recordLength, (char *)page + offset, sizeof(int));

    // for (int i = slotNum + 1; i < nSlot; ++i)
    // {
    //     void *data = malloc(PAGE_SIZE);
    //     int oldOffset;
    //     memcpy(&oldOffset, (char *)page + PAGE_SIZE - (i + 2) * sizeof(int), sizeof(int));
    //     int oldRecordLength;
    //     memcpy(&oldRecordLength, (char *)page +  oldOffset, sizeof(int));
    //     memcpy(data, (char *)page +  oldOffset, oldRecordLength);

    //     memcpy((char *)page + offset, data, oldRecordLength);
    //     memcpy((char *)page + PAGE_SIZE - (i + 2) * sizeof(int), &offset, sizeof(int));
    //     offset += oldRecordLength;
    //     free(data);
    // }
    this->moveRecords(offset, page, slotNum + 1, LEFT);

    int size = -1;
    memcpy((char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), &size, sizeof(int));
    total -= recordLength;
    memcpy((char *)page, &total, sizeof(int));
    fileHandle.writePage(cPage, page);  
    free(page);
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    int nFields = recordDescriptor.size();
    int cPage = rid.pageNum;
    int slotNum = rid.slotNum;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(cPage, page);

    int nSlot;
    //Get total number of slots
    memcpy(&nSlot, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));

    int total;
    int *indexList = new int[nFields];
    this->getIndexList(recordDescriptor, data, indexList);

    memcpy(&total, (char *)page, sizeof(int));
    int recordLength = this->getRecordLength(recordDescriptor, data);
    int left = PAGE_SIZE - total;
    int oldRecordLength;
    int offset;
    memcpy(&offset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
    memcpy(&oldRecordLength, (char *)page + offset, sizeof(int));

#ifdef DEBUG
    printf("updateRecord; record offset: %d, old record length: %d, new record length:%d\n", offset, 
        oldRecordLength, recordLength + (nFields + 1) * sizeof(int));
#endif
    if (recordLength + (nFields + 1) * sizeof(int) == oldRecordLength)
    {
        for (int i = 0; i < nFields; ++i)
        {
            memcpy((char *)page + offset + (i + 1) * sizeof(int), &indexList[i], sizeof(int));
        }
        memcpy((char *)page + offset + (nFields + 1) * sizeof(int), data, recordLength);
        fileHandle.writePage(cPage, page);   
    } else if (recordLength + (nFields + 1) * sizeof(int) < oldRecordLength)
    {
        for (int i = 0; i < nFields; ++i)
        {
            memcpy((char *)page + offset + (i + 1) * sizeof(int), &indexList[i], sizeof(int));
        }
        recordLength += (nFields + 1) * sizeof(int);
        memcpy((char *)page + offset, &recordLength, sizeof(int));
        memcpy((char *)page + offset + (nFields + 1) * sizeof(int), data, recordLength - (nFields + 1) * sizeof(int));

        total = total + (recordLength + (nFields + 1) * sizeof(int) - oldRecordLength);
        memcpy((char *)page, &total, sizeof(int));

        offset += recordLength;

        this->moveRecords(offset, page, slotNum + 1, LEFT);
        fileHandle.writePage(cPage, page);   
    } else if (recordLength + (nFields + 1) * sizeof(int) - oldRecordLength > left)
    {
        RID tempRID;
        this->insertRecord(fileHandle, recordDescriptor, data, tempRID);
        int size = -1;
        memcpy((char *)page + offset, &size, sizeof(int));
        if (oldRecordLength >= 3 * sizeof(int))
        {
            memcpy((char *)page + offset + sizeof(int), &tempRID.pageNum, sizeof(int));
            memcpy((char *)page + offset + 2 * sizeof(int), &tempRID.slotNum, sizeof(int));
            this->moveRecords(offset + 3 * sizeof(int), page, slotNum + 1, LEFT);
        } else
        {
            this->moveRecords(3 * sizeof(int) - oldRecordLength, page, slotNum + 1, RIGHT);
            memcpy((char *)page + offset + sizeof(int), &tempRID.pageNum, sizeof(int));
            memcpy((char *)page + offset + 2 * sizeof(int), &tempRID.slotNum, sizeof(int));
        }
        total = total + 3 * sizeof(int) - oldRecordLength;
        memcpy((char *)page, &total, sizeof(int));
    } else
    {
        this->moveRecords(recordLength + (nFields + 1) * sizeof(int) - oldRecordLength, page, slotNum + 1, RIGHT);
        for (int i = 0; i < nFields; ++i)
        {
            memcpy((char *)page + offset + (i + 1) * sizeof(int), &indexList[i], sizeof(int));
        }
        recordLength += (nFields + 1) * sizeof(int);
        memcpy((char *)page + offset, &recordLength, sizeof(int));
        memcpy((char *)page + offset + (nFields + 1) * sizeof(int), data, recordLength - (nFields + 1) * sizeof(int));

        total = total + (recordLength + (nFields + 1) * sizeof(int) - oldRecordLength);
        memcpy((char *)page, &total, sizeof(int));

        offset += recordLength;
        fileHandle.writePage(cPage, page);   
    }
    free(page);
    return 0;
}

RC RecordBasedFileManager::moveRecords(int offset, void *page, int slotNum, Direction direction)
{
    //If move left, then offset represents the start of the first move,
    //If move right, then offset represents the right shift of the data.

    int nSlot;
    //Get total number of slots
    memcpy(&nSlot, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
    if (direction == LEFT)
    {
        for (int i = slotNum; i < nSlot; ++i)
        {
            void *data = malloc(PAGE_SIZE);
            int oldOffset;
            memcpy(&oldOffset, (char *)page + PAGE_SIZE - (i + 2) * sizeof(int), sizeof(int));
            if (oldOffset == -1)
                continue;
            int oldRecordLength;
            memcpy(&oldRecordLength, (char *)page +  oldOffset, sizeof(int));
            memcpy(data, (char *)page +  oldOffset, oldRecordLength);

            memcpy((char *)page + offset, data, oldRecordLength);
            memcpy((char *)page + PAGE_SIZE - (i + 2) * sizeof(int), &offset, sizeof(int));
            offset += oldRecordLength;
            free(data);
        }
    } else if (direction == RIGHT)
    {
        int shift = offset;
        for (int i = nSlot - 1; i >= slotNum; --i)
        {
            void *data = malloc(PAGE_SIZE);
            int oldOffset;
            memcpy(&oldOffset, (char *)page + PAGE_SIZE - (i + 2) * sizeof(int), sizeof(int));
            if (oldOffset == -1)
                continue;
            int oldRecordLength;
            memcpy(&oldRecordLength, (char *)page +  oldOffset, sizeof(int));
            memcpy(data, (char *)page +  oldOffset, oldRecordLength);

            offset = oldOffset + shift;
            memcpy((char *)page + offset, data, oldRecordLength);
            memcpy((char *)page + PAGE_SIZE - (i + 2) * sizeof(int), &offset, sizeof(int));
            free(data);   
        }
    }
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
    int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    void* content = malloc(PAGE_SIZE);
    this->readRecord(fileHandle, recordDescriptor, rid, content);

    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
    {
        nullFieldsIndicator[i] = ((char *)content)[i];
    }
    int offset = nullFieldsIndicatorActualSize;
    // printf("Number of fields: %d, actual field size: %d, %d\n", nFields, nullFieldsIndicatorActualSize, nullFieldsIndicator[0]);
    int i;
    for (i = 0; i < nFields; ++i)
    {
        if (recordDescriptor[i].name == attributeName)
            break;
    }

    //Not found the attribute
    if (i == nFields)
        return -1;

    int nByte = i / 8;
    int nBit = i % 8;
    bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));

    if(!nullBit)
    {
        int cPage = rid.pageNum;
        int slotNum = rid.slotNum;
        void *page = malloc(PAGE_SIZE);
        fileHandle.readPage(cPage, page);
        int recordOffset;
        memcpy(&recordOffset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
        memcpy(&offset, (char *)page + recordOffset + (1 + i) * sizeof(int), sizeof(int));
#ifdef DEBUG
        printf("record offset: %d, attribute offset: %d, i: %d\n", recordOffset, offset, i);
#endif
        if (recordDescriptor[i].type == TypeInt)
        {
            memcpy(data, (char *)content + offset, recordDescriptor[i].length);
        }
        if (recordDescriptor[i].type == TypeReal)
        {
            memcpy(data, (char *)content + offset, recordDescriptor[i].length);
        }
        if (recordDescriptor[i].type == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, (char *)content + offset, sizeof(int));
            // printf("String length: %d\n", nameLength);
            memcpy(data, (char *)content + offset, nameLength);
        }          
        free(page);
    }
    free(content);
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
  const vector<Attribute> &recordDescriptor,
  const string &conditionAttribute,
  const CompOp compOp,                  // comparision type such as "<" and "="
  const void *value,                    // used in the comparison
  const vector<string> &attributeNames, // a list of projected attributes
  RBFM_ScanIterator &rbfm_ScanIterator)
{
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = value;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.attributePositions = new int[attributeNames.size()];
    rbfm_ScanIterator.fileHandle = &fileHandle;
    rbfm_ScanIterator.attributeNames = attributeNames;

    for (int i = 0; i < attributeNames.size(); ++i)
    {
        int j;
        for (j = 0; i < recordDescriptor.size(); ++j)
        {
            if (recordDescriptor[j].name == attributeNames[i])
            {
                rbfm_ScanIterator.attributePositions[i] = j;
                break;
            }
        }
        if (j == recordDescriptor.size())
            return -1;
    }
    int i;
    for (i = 0; i < recordDescriptor.size(); ++i)
    {
        if (recordDescriptor[i].name == conditionAttribute)
        {
            rbfm_ScanIterator.conditionAttributePosition = i;
            break;
        }
    }
    if (i == recordDescriptor.size())
        return -1;
    return 0;
}

RBFM_ScanIterator::RBFM_ScanIterator()
{
    cPage = 0;
    cSlot = -1;
}

RC RBFM_ScanIterator::close()
{
    cPage = 0;
    cSlot = -1;
    return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    void * page = malloc(PAGE_SIZE);
    fileHandle->readPage(cPage, page);
    int nSlots;
    memcpy(&nSlots, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));

    while (1)
    {
        cSlot++;
#ifdef DEBUG
        printf("current page: %d, current slot:%d, total pages: %d, total slots: %d\n", cPage, cSlot, fileHandle->getNumberOfPages(), nSlots);
#endif
        if (cSlot > nSlots - 1)
        {
            cPage++;
            cSlot = 0;
            fileHandle->readPage(cPage, page);
            memcpy(&nSlots, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
        }
        if (cPage > fileHandle->getNumberOfPages() - 1)
        {
            return RBFM_EOF;
        }

        int offset;
        
        bool valid = true;
        bool satisfied = false;
        memcpy(&offset, (char *)page + PAGE_SIZE - (cSlot + 2) * sizeof(int), sizeof(int));
        if (offset == -1)
            valid = false;
        rid.slotNum = cSlot;
        rid.pageNum = cPage;

        if (valid)
        {
            int offset;
            memcpy(&offset, (char *)page + PAGE_SIZE - (cSlot + 2) * sizeof(int), sizeof(int));

            //Has been deleted
            if (offset == -1)
                return 1;
            int recordLength;
            memcpy(&recordLength, (char *)page + offset, sizeof(int));

            //Update to new place
            while (recordLength == -1)
            {
                int tPage;
                int tSlot;
                memcpy(&tPage, (char *)page + offset + sizeof(int), sizeof(int));
                memcpy(&tSlot, (char *)page + offset + 2 * sizeof(int), sizeof(int));
                fileHandle->readPage(tPage, page);
                memcpy(&offset, (char *)page + PAGE_SIZE - (tSlot + 2) * sizeof(int), sizeof(int));
                memcpy(&recordLength, (char *)page + offset, sizeof(int));
            }
            int nFields = recordDescriptor.size();
            int dataOffset = offset + (nFields + 1) * sizeof(int);
            int attrOffset;
            memcpy(&attrOffset, (char *)page + offset + (conditionAttributePosition + 1) * sizeof(int), sizeof(int));
#ifdef DEBUG
            printf("offset: %d, dataOffset: %d, attrOffset: %d, conditionAttributePosition: %d\n",
                offset, dataOffset, attrOffset, conditionAttributePosition);
#endif
            if (recordDescriptor[conditionAttributePosition].type == TypeInt)
            {
                int value;
                memcpy(&value, (char *)page + dataOffset + attrOffset, recordDescriptor[conditionAttributePosition].length);
                int searchValue;
                memcpy(&searchValue, this->value, recordDescriptor[conditionAttributePosition].length);
#ifdef DEBUG
                printf("value: %d, searchValue: %d\n", value, searchValue);
#endif
                
                switch (compOp)
                {
                    case EQ_OP:
                        satisfied = value == searchValue;
                        break;
                    case LT_OP:
                        satisfied = value < searchValue;
                        break;
                    case LE_OP:
                        satisfied = value <= searchValue;
                        break;
                    case GT_OP:
                        satisfied = value > searchValue;
                        break;
                    case GE_OP:
                        satisfied = value >= searchValue;
                        break;
                    case NE_OP:
                        satisfied = value != searchValue;
                        break;
                    case NO_OP:
                        satisfied = true;
                        break;
                }
            }
            if (recordDescriptor[conditionAttributePosition].type == TypeReal)
            {
                float value;
                memcpy(&value, (char *)page + dataOffset + attrOffset, recordDescriptor[conditionAttributePosition].length);
                float searchValue;
                memcpy(&searchValue, this->value, recordDescriptor[conditionAttributePosition].length);
                
                switch (compOp)
                {
                    case EQ_OP:
                        satisfied = value == searchValue;
                        break;
                    case LT_OP:
                        satisfied = value < searchValue;
                        break;
                    case LE_OP:
                        satisfied = value <= searchValue;
                        break;
                    case GT_OP:
                        satisfied = value > searchValue;
                        break;
                    case GE_OP:
                        satisfied = value >= searchValue;
                        break;
                    case NE_OP:
                        satisfied = value != searchValue;
                        break;
                    case NO_OP:
                        satisfied = true;
                        break;
                }
            }
            if (recordDescriptor[conditionAttributePosition].type == TypeVarChar)
            {
                int nameLength;
                memcpy(&nameLength, (char *)page + dataOffset + attrOffset, sizeof(int));
                char* value_c = (char *) malloc(nameLength + 1);
                memcpy(value_c, (char *)page + dataOffset + attrOffset + sizeof(int), nameLength);
                value_c[nameLength] = '\0';
                string searchValue = string((char *) this->value);
                string value = string(value_c);
#ifdef DEBUG
                printf("name length: %d, value: %s, searchValue: %s\n", nameLength, value.c_str(), searchValue.c_str());
#endif

                switch (compOp)
                {
                    case EQ_OP:
                        satisfied = value == searchValue;
                        break;
                    case LT_OP:
                        satisfied = value < searchValue;
                        break;
                    case LE_OP:
                        satisfied = value <= searchValue;
                        break;
                    case GT_OP:
                        satisfied = value > searchValue;
                        break;
                    case GE_OP:
                        satisfied = value >= searchValue;
                        break;
                    case NE_OP:
                        satisfied = value != searchValue;
                        break;
                    case NO_OP:
                        satisfied = true;
                        break;
                }
            }

            if (satisfied)
            {
#ifdef DEBUG
                printf("Satisfied!\n");
#endif
                int nFields = attributeNames.size();
                int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
                unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
                int returnDataOffset = nullFieldsIndicatorActualSize;
                memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
                for (int i = 0; i < attributeNames.size(); ++i)
                {
                    memcpy(&attrOffset, (char *)page + offset + (attributePositions[i] + 1) * sizeof(int), sizeof(int));
                    if (attrOffset == -1)
                    {
                        int nByte = i / 8;
                        int nBit = i % 8;
                        nullsIndicator[nByte] |= (1 << (7 - nBit));
                        // bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit)); 
                    } else
                    {
                        if (recordDescriptor[attributePositions[i]].type == TypeInt)
                        {
                            int value;
                            memcpy(&value, (char *)page + dataOffset + attrOffset, recordDescriptor[attributePositions[i]].length);
                            memcpy((char *)data + returnDataOffset, &value, recordDescriptor[attributePositions[i]].length);
                            returnDataOffset += recordDescriptor[attributePositions[i]].length;
                        }
                        if (recordDescriptor[attributePositions[i]].type == TypeReal)
                        {
                            float value;
                            memcpy(&value, (char *)page + dataOffset + attrOffset, recordDescriptor[attributePositions[i]].length);
                            memcpy((char *)data + returnDataOffset, &value, recordDescriptor[attributePositions[i]].length);
                            returnDataOffset += recordDescriptor[attributePositions[i]].length;
                        }
                        if (recordDescriptor[attributePositions[i]].type == TypeVarChar)
                        {
                            int nameLength;
                            memcpy(&nameLength, (char *)page + dataOffset + attrOffset, sizeof(int));
                            memcpy((char *)data + returnDataOffset, &nameLength, sizeof(int));
                            returnDataOffset += sizeof(int);
                            
                            char* value = (char *) malloc(nameLength);
                            memcpy(value, (char *)page + dataOffset + attrOffset + sizeof(int), nameLength);
                            memcpy((char *)data + returnDataOffset, value, nameLength);
                            returnDataOffset += nameLength;
                        }   
                    }
                }
                memcpy((char *)data, nullsIndicator, nullFieldsIndicatorActualSize);
                return 0;
            }
        }
    }
    free(page);
}
