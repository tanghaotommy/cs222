
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) 
{
	this->input = input;
	this->condition = &condition;
	input->getAttributes(this->attrs);

	for (int i = 0; i < this->attrs.size(); ++i)
	{
// 		vector<string> strs;
// 		split(strs, this->attrs[i].name);
// 		if (strs.size() != 2)
// 		{
// #ifdef DEBUG
// 			printf("[Filter] Initiliazation failed because of invalid split, split size: %d\n", strs.size());
// #endif
// 		}
// 		string attributeName = strs[this->attrs.size() - 1];
		int pos = attrs[i].name.find('.');
		string attributeName = attrs[i].name.substr(pos + 1, attrs[i].name.length() - pos + 1);
		this->attrs[i].name = attributeName;
	}
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = this->attrs;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = "Filter";
        tmp += ".";
        tmp += attrs.at(i).name;
        attrs.at(i).name = tmp;
    }
}


RC Filter::getNextTuple(void *data)
{
	while (this->input->getNextTuple(data) != RM_EOF)
	{
#ifdef DEBUG_QE
		RelationManager::instance()->printTuple(this->attrs, data);
#endif

		if (this->condition->bRhsIsAttr)
		{
#ifdef DEBUG_QE
			printf("[Filter::getNextTuple]Right hand is an attribute\n");
			return QE_EOF;
#endif
		}
		else
		{
			void *value = malloc(PAGE_SIZE);
			int pos = getValueOfAttrByName(data, this->attrs, getOriginalAttrName(this->condition->lhsAttr), value);

#ifdef DEBUG_QE
			int v;
			memcpy(&v, value, sizeof(int));
			printf("[Filter::getNextTuple] return value from getValueOfAttrByName: %d\n", v);
#endif


			if (pos == -1 || pos == -2)
			{
#ifdef DEBUG_QE
				printf("[Filter::getNextTuple] position of the attribute: %d\n", pos);
#endif
				free(value);
				continue;
			}

			bool satisfied = compareCondition(&this->attrs[pos], value, this->condition);
			free(value);
			if (satisfied)
				return 0;
		}
	}
	return QE_EOF;
}

Project::Project(Iterator *input, const vector<string> &attrNames)
{
	this->input = input;
	input->getAttributes(this->attrs);

	for (int i = 0; i < this->attrs.size(); ++i)
	{
		int pos = attrs[i].name.find('.');
		string attributeName = attrs[i].name.substr(pos + 1, attrs[i].name.length() - pos + 1);
		this->attrs[i].name = attributeName;
	}

	for (int i = 0; i < attrNames.size(); ++i)
	{
		int pos = attrNames[i].find('.');
		string attributeName = attrNames[i].substr(pos + 1, attrNames[i].length() - pos + 1);

		for (int j = 0; j < this->attrs.size(); ++j)
		{
			if (attributeName == this->attrs[j].name)
			{
				this->projectAttrs.push_back(this->attrs[j]);
				break;
			}
		}
	}
	if (this->projectAttrs.size() != attrNames.size())
	{
#ifdef DEBUG_QE
		printf("[Project::Project] Some attrNames can not be found!\n");
#endif
	}
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
    attrs = this->projectAttrs;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = "Project";
        tmp += ".";
        tmp += attrs.at(i).name;
        attrs.at(i).name = tmp;
    }
}

RC Project::getNextTuple(void *data)
{
	void* temp = malloc(PAGE_SIZE);
	if (this->input->getNextTuple(temp) != RM_EOF)
	{
#ifdef DEBUG_QE
		RelationManager::instance()->printTuple(this->attrs, temp);
#endif
		int nFields = this->projectAttrs.size();
	    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);

	    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
	    {
	        nullFieldsIndicator[i] = 0;
	    }
	    int offset = nullFieldsIndicatorActualSize;
	    for (int i = 0; i < this->projectAttrs.size(); ++i)
	    {
	    	int nByte = i / 8;
    		int nBit = i % 8;
    		void *value = malloc(PAGE_SIZE);
    		int rc = getValueOfAttrByName(temp, this->attrs, this->projectAttrs[i].name, value);
    		if (rc == -1)
    		{
    			nullFieldsIndicator[nByte] |= (1 << (7 - nBit));
    		}
    		else
    		{
    			if (this->projectAttrs[i].type == TypeInt)
	            {
	            	// int v;
	                memcpy((char *)data + offset, value, this->projectAttrs[i].length);
	                // memcpy(&v, value, sizeof(int));
	                offset += this->projectAttrs[i].length;
	                // printf("[getValueOfAttrByName]%s: %-10d\n", attrs[i].name.c_str(), value);
	            }
	            if (this->projectAttrs[i].type == TypeReal)
	            {
	                memcpy((char *)data + offset, value, this->projectAttrs[i].length);
	                offset += this->projectAttrs[i].length;
	                // printf("[getValueOfAttrByName]%s: %-10f\n", attrs[i].name.c_str(), value);
	            }
	            if (this->projectAttrs[i].type == TypeVarChar)
	            {
	                int nameLength;
	                memcpy(&nameLength, value, sizeof(int));
	                memcpy((char *)data + offset, value, sizeof(int));
	                offset += sizeof(int);
	                // printf("String length: %d\n", nameLength);
	                memcpy((char *)data + offset, (char *)value + sizeof(int), nameLength);
	                offset += nameLength;
	                //printf("%s: %-10s\t", attrs[i].name.c_str(), value);
	            }      
    		}
    		free(value);
	    }
	    memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	    free(nullFieldsIndicator);
	    free(temp);
	    return 0;
	} else
	{
		free(temp);
		return QE_EOF;
	}
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op)
{
	this->input = input;
	this->hasGroupBy = false;
	input->getAttributes(this->attrs);
	this->op = op;
	this->aggAttr = aggAttr;
	this->total = 1;

	for (int i = 0; i < this->attrs.size(); ++i)
	{
		int pos = attrs[i].name.find('.');
		string attributeName = attrs[i].name.substr(pos + 1, attrs[i].name.length() - pos + 1);
		this->relation = attrs[i].name.substr(0, pos);
		this->attrs[i].name = attributeName;
	}
#ifdef DEBUG_QE
	printf("[Aggregate::Aggregate] Relation attirbute prefix: %s\n", this->relation.c_str());
#endif
	int pos = aggAttr.name.find('.');
	this->aggAttr.name = aggAttr.name.substr(pos + 1, aggAttr.name.length() - pos + 1);

	getAggregateResults();
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr, AggregateOp op)
{
	this->input = input;
	this->hasGroupBy = true;
	input->getAttributes(this->attrs);
	this->op = op;
	this->aggAttr = aggAttr;
	this->total = 1;

	for (int i = 0; i < this->attrs.size(); ++i)
	{
		int pos = attrs[i].name.find('.');
		string attributeName = attrs[i].name.substr(pos + 1, attrs[i].name.length() - pos + 1);
		this->relation = attrs[i].name.substr(0, pos);
		this->attrs[i].name = attributeName;
	}
#ifdef DEBUG_QE
	printf("[Aggregate::Aggregate] Relation attirbute prefix: %s\n", this->relation.c_str());
#endif
	int pos = aggAttr.name.find('.');
	this->aggAttr.name = aggAttr.name.substr(pos + 1, aggAttr.name.length() - pos + 1);

	this->groupAttr = groupAttr;

	pos = groupAttr.name.find('.');
	this->groupAttr.name = groupAttr.name.substr(pos + 1, groupAttr.name.length() - pos + 1);

	getAggregateResults();
}

void Aggregate::getAggregateResults()
{
	void *data = malloc(PAGE_SIZE);
	while(input->getNextTuple(data) != RM_EOF)
	{
#ifdef DEBUG_QE
		RelationManager::instance()->printTuple(this->attrs, data);
#endif
		void *value = malloc(PAGE_SIZE);
		int rc = getValueOfAttrByName(data, this->attrs, this->aggAttr.name, value);
		if (rc == -1)
		{
#ifdef DEBUG_QE
			printf("[Aggregate::getAggregateResults] Null value encountered! What to do with Null?\n");
#endif
		}

		float v;

		switch(this->aggAttr.type)
		{
			case TypeInt:
				int temp;
				memcpy(&temp, value, sizeof(int));
				v = (float) temp;
				break;
			case TypeReal:
				memcpy(&v, value, sizeof(TypeReal));
				break;
		}

		if (this->hasGroupBy)
		{
			void *key_value = malloc(PAGE_SIZE);
			rc = getValueOfAttrByName(data, this->attrs, this->groupAttr.name, key_value);

			switch(this->groupAttr.type)
			{
				case TypeInt:
				{
					int intKey;
					memcpy(&intKey, key_value, sizeof(int));

					unordered_map<int, GroupAttr>::iterator gotInt = this->intMap.find (intKey);
					if (gotInt == this->intMap.end())
					{
						GroupAttr temp;
						temp.sum += v;
						temp.count++;
						intMap[intKey] = temp;
					}
					else
					{
						gotInt->second.sum += v;
						gotInt->second.count++;
						gotInt->second.max = v > (gotInt->second.max) ? v : gotInt->second.max;
						gotInt->second.min = v < (gotInt->second.min) ? v : gotInt->second.min;
					}
					break;
				}
				case TypeReal:
				{
					float floatKey;
					memcpy(&floatKey, key_value, sizeof(float));

					unordered_map<float, GroupAttr>::iterator gotFloat = this->floatMap.find (floatKey);
					if (gotFloat == this->floatMap.end())
					{
						GroupAttr temp;
						floatMap[floatKey] = temp;
						temp.sum += v;
						temp.count++;
					}
					else
					{
						gotFloat->second.sum += v;
						gotFloat->second.count++;
						gotFloat->second.max = v > (gotFloat->second.max) ? v : gotFloat->second.max;
						gotFloat->second.min = v < (gotFloat->second.min) ? v : gotFloat->second.min;
					}
					break;
				}
				case TypeVarChar:
				{
					int nameLength;
					char* key_c = (char *) malloc(nameLength + 1);
					memcpy(&nameLength, key_value, sizeof(int));
					memcpy(key_c, key_value, nameLength);

					key_c[nameLength] = '\0';
					string stringKey = string(key_c);

					unordered_map<string, GroupAttr>::iterator gotString = this->stringMap.find (stringKey);
					if (gotString == this->stringMap.end())
					{
						GroupAttr temp;
						stringMap[stringKey] = temp;
						temp.sum += v;
						temp.count++;
					}
					else
					{
						gotString->second.sum += v;
						gotString->second.count++;
						gotString->second.max = v > (gotString->second.max) ? v : gotString->second.max;
						gotString->second.min = v < (gotString->second.min) ? v : gotString->second.min;
					}
					break;
				}
			}

			switch(this->groupAttr.type)
			{
				case TypeInt:
					this->intIterator = this->intMap.begin();
					this->total = intMap.size();
					break;
				case TypeReal:
					this->floatIterator = this->floatMap.begin();
					this->total = floatMap.size();
					break;
				case TypeVarChar:
					this->stringIterator = this->stringMap.begin();
					this->total = stringMap.size();
					break;
			}
			free(key_value);
		} 
		else
		{
			this->count++;
			this->sum += v;
			if (v > this->max)
				this->max = v;
			if (v < this->min)
				this->min = v;
		}
		free(value);
	}

	free(data);
}

RC Aggregate::getNextTuple(void *data)
{
	printf("total number of keys: %d\n", this->total);
	if (current >= total)
		return QE_EOF;

	int nFields = 1;
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    nullFieldsIndicator[0] = 0;
    int offset = nullFieldsIndicatorActualSize;
    float res;

    if (hasGroupBy)
    {
    	GroupAttr groupRes;
    	switch(this->groupAttr.type)
		{
			case TypeInt:
				memcpy((char *)data + offset, &(intIterator->first), sizeof(int));
				offset += sizeof(int);
				groupRes = intIterator->second;
				intIterator++;
				break;
			case TypeReal:
				memcpy((char *)data + offset, &(floatIterator->first), sizeof(float));
				offset += sizeof(float);
				groupRes = floatIterator->second;
				floatIterator++;
				break;
			case TypeVarChar:
				int nameLength = stringIterator->first.length();
				memcpy((char *)data + offset, &nameLength, sizeof(int));
				offset += sizeof(int);
				memcpy((char *)data + offset, &(stringIterator->first), sizeof(int));
				offset += sizeof(nameLength);
				groupRes = stringIterator->second;
				stringIterator++;
				break;
		}

		switch (this->op)
		{
			case MIN:
				res = groupRes.min;
				break;
			case MAX:
				res = groupRes.max;
				break;
			case COUNT:
				res = groupRes.count;
				break;
			case AVG:
				res = groupRes.sum / groupRes.count;
				break;
			case SUM:
				res = groupRes.sum;
				break;
    	} 
    }
    else 
    {
		switch (this->op)
		{
			case MIN:
				res = this->min;
				break;
			case MAX:
				res = this->max;
				break;
			case COUNT:
				res = this->count;
				break;
			case AVG:
				res = this->sum / this->count;
				break;
			case SUM:
				res = this->sum;
				break;
		}
	}

	memcpy((char*) data + offset, &res, sizeof(float));
	offset += sizeof(float);
    current++;

    memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    free(nullFieldsIndicator);
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();

	if (hasGroupBy)
	{
	    string tmp = this->relation + "." + this->groupAttr.name;
	    Attribute attr = this->groupAttr;
	    attr.name = tmp;
		attrs.push_back(attr);
	}

	//Put aggregate op name;
	string tmp = getOpName();
    tmp += "(";
    tmp += this->relation + "." + this->aggAttr.name;
    tmp += ")";
    Attribute attr = this->aggAttr;
    attr.name = tmp;
	attrs.push_back(attr);
}

string Aggregate::getOpName() const
{
	switch (this->op)
	{
		case MIN:
			return "MIN";
			break;
		case MAX:
			return "MAX";
			break;
		case COUNT:
			return "COUNT";
			break;
		case AVG:
			return "AVG";
			break;
		case SUM:
			return "SUM";
			break;
	}
}


INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition)
{
	this->condition = &condition;
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	leftIn->getAttributes(this->leftAttributes);

	for (int i = 0; i < this->leftAttributes.size(); ++i)
	{
		int pos = this->leftAttributes[i].name.find('.');
		string attributeName = this->leftAttributes[i].name.substr(pos + 1, this->leftAttributes[i].name.length() - pos + 1);
		this->leftTable = this->leftAttributes[i].name.substr(0, pos);
		this->leftAttributes[i].name = attributeName;
	}

	rightIn->getAttributes(this->rightAttributes);

	for (int i = 0; i < this->rightAttributes.size(); ++i)
	{
		int pos = this->rightAttributes[i].name.find('.');
		string attributeName = this->rightAttributes[i].name.substr(pos + 1, this->rightAttributes[i].name.length() - pos + 1);
		this->rightTable = this->rightAttributes[i].name.substr(0, pos);
		this->rightAttributes[i].name = attributeName;
	}
}
INLJoin::~INLJoin()
{
	if (leftData != NULL)
		free(leftData);
}

RC INLJoin::getNextTuple(void *data)
{
	void *rightData = malloc(PAGE_SIZE);
	while(1)
	{
		if (this->leftData == NULL)
		{
#ifdef DEBUG_QE
			printf("[INLJoin::getNextTuple] Loading tuple from left input\n");
#endif
			this->leftData = malloc(PAGE_SIZE);
			if (this->leftIn->getNextTuple(this->leftData) == QE_EOF)
				return QE_EOF;
#ifdef DEBUG_QE
			RelationManager::instance()->printTuple(this->leftAttributes, this->leftData);
#endif
		}

		if (this->rightIn->getNextTuple(rightData) != QE_EOF)
		{
#ifdef DEBUG_QE
			printf("[INLJoin::getNextTuple] Loading tuple from right input\n");
			RelationManager::instance()->printTuple(this->rightAttributes, rightData);
#endif
			bool can = canJoin(this->leftData, rightData);
			if (can)
			{
#ifdef DEBUG_QE
        	                printf("[INLJoin::getNextTuple] Concatenating left and right, left data is: \n");
      	          	        RelationManager::instance()->printTuple(this->leftAttributes, this->leftData);
#endif
				concatenateLeftAndRight(this->leftData, rightData, data);
				free(rightData);
				return 0;
			}
			else
				continue;
		} 
		else
		{
			if (this->leftIn->getNextTuple(this->leftData) != QE_EOF)
			{
#ifdef DEBUG_QE
				printf("[INLJoin::getNextTuple] Loading tuple from left input\n");
				RelationManager::instance()->printTuple(this->leftAttributes, this->leftData);
#endif
				this->rightIn->setIterator(NULL, NULL, true, true);
				continue;
			} 
			else
			{
				return QE_EOF;
			}
		}
	}
	free(rightData);
}

bool INLJoin::canJoin(const void* leftData, const void* rightData)
{
	if (this->condition->bRhsIsAttr)
	{
		string leftAttrName = getOriginalAttrName(this->condition->lhsAttr);
		string rightAttrName = getOriginalAttrName(this->condition->rhsAttr);
		void *leftValue = malloc(PAGE_SIZE);
		void *rightValue = malloc(PAGE_SIZE);

		int leftPos = getValueOfAttrByName(leftData, this->leftAttributes, leftAttrName, leftValue);
		int rightPos = getValueOfAttrByName(rightData, this->rightAttributes, rightAttrName, rightValue);

		bool res;
		if (isEqual(leftValue, rightValue, &this->leftAttributes[leftPos]))
			res = true;
		else
			res = false;

		free(leftValue);
		free(rightValue);
		return res;
	}
	return false;
}

void INLJoin::concatenateLeftAndRight(const void* leftData, const void* rightData, void* data)
{
#ifdef DEBUG_QE
    printf("[concatenateLeftAndRight] concatenate between:\n");
    RelationManager::instance()->printTuple(this->leftAttributes, this->leftData);
    RelationManager::instance()->printTuple(this->rightAttributes, rightData);
#endif
	int nLeftFields = this->leftAttributes.size();
    int nullLeftFieldsIndicatorActualSize = ceil((double) nLeftFields / CHAR_BIT);
    unsigned char *nullLeftFieldsIndicator = (unsigned char *)malloc(nullLeftFieldsIndicatorActualSize);
    memcpy(nullLeftFieldsIndicator, leftData, nullLeftFieldsIndicatorActualSize);

	int nRightFields = this->rightAttributes.size();
    int nullRightFieldsIndicatorActualSize = ceil((double) nRightFields / CHAR_BIT);
    unsigned char *nullRightFieldsIndicator = (unsigned char *)malloc(nullRightFieldsIndicatorActualSize);
    memcpy(nullRightFieldsIndicator, rightData, nullRightFieldsIndicatorActualSize);

    int nFields = this->leftAttributes.size() + this->rightAttributes.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);

    int newOffset = nullFieldsIndicatorActualSize;
    int leftOffset = nullLeftFieldsIndicatorActualSize;
    int rightOffset = nullRightFieldsIndicatorActualSize;

    for (int i = 0; i < nFields; ++i)
    {
    	if (i < nLeftFields)
    	{
    		int nByte = i / 8;
	        int nBit = i % 8;
	        bool nullBit = nullLeftFieldsIndicator[nByte] & (1 << (7 - nBit));
#ifdef DEBUG_QE
		printf("[concatenateLeftAndRight] %d field is null ? %d\n", i, nullBit);
#endif
	        if (nullBit)
	        	nullFieldsIndicator[nByte] |= (1 << (7 - nBit));
	        else
	        {
	        	copyAttribute(leftData, leftOffset, data, newOffset, this->leftAttributes[i]);
	        }
    	}
    	else
    	{
    		int nByte = i / 8;
    		int nBit = i % 8;
    		int nRightByte = (i - nLeftFields) / 8;
    		int nRightBit = (i - nLeftFields) % 8;
    		bool nullBit = nullRightFieldsIndicator[nRightByte] & (1 << (7 - nRightBit));
#ifdef DEBUG_QE
                printf("[concatenateLeftAndRight] %d field is null ? %d\n", i, nullBit);
#endif

    		if (nullBit)
	        	nullFieldsIndicator[nByte] |= (1 << (7 - nBit));
	        else
	        {
	        	copyAttribute(rightData, rightOffset, data, newOffset, this->rightAttributes[i - nLeftFields]);
	        }
    	}
    }

    memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
#ifdef DEBUG_QE
    printf("[concatenateLeftAndRight] After concatenation\n");
    RelationManager::instance()->printTuple(this->leftAttributes, data);
#endif
    free(nullLeftFieldsIndicator);
    free(nullRightFieldsIndicator);
    free(nullFieldsIndicator);
}

void copyAttribute(const void *from, int &fromOffset, void* to, int &toOffset, const Attribute &attr)
{
#ifdef DEBUG_QE
	printf("[copyAttribute] Copying attribute %s, fromOffset: %d, toOffset: %d\n", attr.name.c_str(), fromOffset, toOffset);
#endif
	switch(attr.type)
	{
		case TypeInt:
#ifdef DEBUG_QE
			int t;
			memcpy(&t, (char *)from + fromOffset, attr.length);
			printf("[copyAttribute] Value of attribute is %d\n", t);
#endif
			memcpy((char *)to + toOffset, (char *)from + fromOffset, attr.length);
			fromOffset += attr.length;
			toOffset += attr.length;
			break;
		case TypeReal:
#ifdef DEBUG_QE
                        float f;
                        memcpy(&f, (char *)from + fromOffset, attr.length);
                        printf("[copyAttribute] Value of attribute is %f\n", f);
#endif
			memcpy((char *)to + toOffset, (char *)from + fromOffset, attr.length);
			fromOffset += attr.length;
			toOffset += attr.length;
			break;
		case TypeVarChar:
			int nameLength;
			memcpy(&nameLength, (char *)from + fromOffset, sizeof(int));
			memcpy((char *)to + toOffset, &nameLength, sizeof(int));
			fromOffset += attr.length;
			toOffset += attr.length;
			memcpy((char *)to + toOffset, (char *)from + fromOffset, nameLength);
			fromOffset += nameLength;
			toOffset += nameLength;
			break;
	}
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
    attrs = this->leftAttributes;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = this->leftTable;
        tmp += ".";
        tmp += attrs.at(i).name;
        attrs.at(i).name = tmp;
    }

    Attribute t;
    for(i = 0; i < this->rightAttributes.size(); ++i)
    {
        string tmp = this->rightTable;
        tmp += ".";
        tmp += this->rightAttributes.at(i).name;

        t = this->rightAttributes[i];
        t.name = tmp;
        attrs.push_back(t);
    }
}

int getValueOfAttrByName(const void *data, vector<Attribute> &attrs, string attributeName, void* value)
{
	int pos;
	for (pos = 0; pos < attrs.size(); ++pos)
	{
		// printf("attributename: %s, attrs[pos].name: %s\n", attributeName.c_str(), attrs[pos].name.c_str());
		if (attrs[pos].name == attributeName)
			break;
	}

	if (pos == attrs.size())
		//attribute not found
		return -2;

#ifdef DEBUG_QE
		printf("[getValueOfAttrByName] position of the attribute %s: %d\n", attributeName.c_str(), pos);
#endif

	int nFields = attrs.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);

    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
    {
        nullFieldsIndicator[i] = ((char *)data)[i];
    }
    int offset = nullFieldsIndicatorActualSize;

    int nByte = pos / 8;
    int nBit = pos % 8;
    bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
    int returnDataOffset = 1;
    int isNull = nullBit ? 1 : 0;

    if (isNull)
    	return -1;
    else
    {
    	for (int i = 0; i < nFields; ++i)
	    {
	        int nByte = i / 8;
	        int nBit = i % 8;
	        bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
	        if(!nullBit)
	        {
	            if (attrs[i].type == TypeInt)
	            {
	            	// int v;
	                memcpy(value, (char *)data + offset, attrs[i].length);
	                // memcpy(&v, value, sizeof(int));
	                offset += attrs[i].length;
	                // printf("[getValueOfAttrByName]%s: %-10d\n", attrs[i].name.c_str(), value);
	            }
	            if (attrs[i].type == TypeReal)
	            {
	                memcpy(value, (char *)data + offset, attrs[i].length);
	                offset += attrs[i].length;
	                // printf("[getValueOfAttrByName]%s: %-10f\n", attrs[i].name.c_str(), value);
	            }
	            if (attrs[i].type == TypeVarChar)
	            {
	                int nameLength;
	                memcpy(&nameLength, (char *)data + offset, sizeof(int));
	                offset += sizeof(int);
	                // printf("String length: %d\n", nameLength);
	                memcpy(value, &nameLength, sizeof(int));
	                memcpy((char *)value + sizeof(int), (char *)data + offset, nameLength);
	                offset += nameLength;

	                //printf("%s: %-10s\t", attrs[i].name.c_str(), value);
	            }          
	         	if (i == pos)
		        {
		        	break;
		        }
	        }
	        else
	        {
	            // printf("%s: NULL\t", attrs[i].name.c_str());
	        }
	    }
    }

    //int v;
    // memcpy(&v, (char *)value, sizeof(int));
    // printf("[getValueOfAttrByName]return value %s: %-10d\n", attrs[pos].name.c_str(), value);
    free(nullFieldsIndicator);
    return pos;
}

bool compareCondition(const Attribute *attribute, const void* value, const Condition* condition)
{
	CompOp compOp = condition->op;
#ifdef DEBUG_QE
	int v;
	memcpy(&v, value, sizeof(int));
	printf("[QE::compareCondition] Value is %d\n", v);
#endif
	bool satisfied = false;
    switch (compOp)
    {
        case EQ_OP:
            satisfied = isEqual(value, condition->rhsValue.data, attribute);
            break;
        case LT_OP:
            satisfied = isLessThan(attribute, value, condition->rhsValue.data);
            break;
        case LE_OP:
            satisfied = isLessAndEqualThan(attribute, value, condition->rhsValue.data);
            // printf("is less and equal than: %d\n", satisfied);
            break;
        case GT_OP:
            satisfied = isLargerThan(attribute, value, condition->rhsValue.data);
            break;
        case GE_OP:
            satisfied = isLargerAndEqualThan(attribute, value, condition->rhsValue.data);
            break;
        case NE_OP:
            satisfied = !isEqual(value, condition->rhsValue.data, attribute);
            break;
        case NO_OP:
            satisfied = true;
            break;
    }
    return satisfied;
}

string getOriginalAttrName(const string s)
{
	int pos = s.find('.');
	return s.substr(pos + 1, s.length() - pos + 1);
}
// ... the rest of your implementations go here
