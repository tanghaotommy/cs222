#include "pfm.h"
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string>
using namespace std;

bool fexists(const char *filename)
{
    ifstream ifile(filename);
    return (bool)ifile;
}

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    if(fexists(fileName.c_str()))
        return 1;
    fstream fs;
    fs.open(fileName, ios::out);
    char *data = new char[PAGE_SIZE];

    //Initialize the counter in the file
    int offset = 0;
    unsigned counter = 0;
    memcpy(data + offset, &counter, sizeof(unsigned));
    offset += sizeof(unsigned);
    memcpy(data + offset, &counter, sizeof(unsigned));
    offset += sizeof(unsigned);
    memcpy(data + offset, &counter, sizeof(unsigned));

    fs.write(data, PAGE_SIZE);
    fs.close();
    delete data;
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if (!fexists(fileName.c_str()))
    {
        return 1;
    }
    remove(fileName.c_str());
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return fileHandle.openFile(fileName);
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return fileHandle.closeFile();
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    nPages = 0;
}

RC FileHandle::readCounter()
{
    void *page = malloc(PAGE_SIZE);

    fs.seekg(0);
    fs.read((char *)page, PAGE_SIZE);
    memcpy(&this->readPageCounter, (char *)page, sizeof(unsigned));
    memcpy(&this->writePageCounter, (char *)page + sizeof(unsigned), sizeof(unsigned));
    memcpy(&this->appendPageCounter, (char *)page + 2 * sizeof(unsigned), sizeof(unsigned));
    // printf("[readCounter] %d, %d, %d\n", this->readPageCounter, this->writePageCounter, this->appendPageCounter);
    free(page);

    return 0;
}

RC FileHandle::writeCounter()
{
    void *page = malloc(PAGE_SIZE);
    memcpy((char *)page, &this->readPageCounter, sizeof(unsigned));
    memcpy((char *)page + sizeof(unsigned), &this->writePageCounter, sizeof(unsigned));
    memcpy((char *)page + 2 * sizeof(unsigned), &this->appendPageCounter, sizeof(unsigned));
    fs.seekg(0);
    fs.write((char *)page, PAGE_SIZE);
    free(page);
    return 0;
}

RC FileHandle::openFile(const string &fileName)
{
    if (fs.is_open())
        return -1;
    try
    {
        if(!fexists(fileName.c_str()))
            return 1;
        fs.open(fileName);
        this->readCounter();
    }
    catch (fstream::failure e)
    {
        cerr << "Exception in open/reading/closing file\n";
        return 1;
    }
    return 0;
}

bool FileHandle::isOpen()
{
    return fs.is_open();
}

RC FileHandle::closeFile()
{
    if (fs.is_open())
    {
        try
        {
            this->writeCounter();
            fs.close();
        }
        catch (fstream::failure e)
        {
            cerr << "Exception in open/reading/closing file\n";
            return 1;
        }
        return 0;
    }
    return 2;
}

FileHandle::~FileHandle()
{
    this->closeFile();
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
#ifdef DEBUG
    printf("[readPage] read page: %d\n", pageNum);
#endif
    if (pageNum >= this->getNumberOfPages())
    {
        return 1;
    }
    //Because there is one hidden page for file information
    fs.seekg((pageNum + 1) * PAGE_SIZE);
    fs.read((char *)data, PAGE_SIZE);
    this->readPageCounter++;
    this->writeCounter();
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
#ifdef DEBUG
    printf("[writePage] writing to page: %d\n", pageNum);
#endif
    if (pageNum >= this->getNumberOfPages())
    {
        return 1;
    }
    //Because there is one hidden page for file information
    fs.seekp((pageNum + 1) * PAGE_SIZE);
    fs.write((char *)data, PAGE_SIZE);
    this->writePageCounter++;
    this->writeCounter();
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    fs.seekp(0, fs.end);
    fs.write((char *)data, PAGE_SIZE);
    this->appendPageCounter++;
    this->writeCounter();
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    fs.seekg(0, fs.end);
    int length = fs.tellg();
    return length / PAGE_SIZE - 1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}
