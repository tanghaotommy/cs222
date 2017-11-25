
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
		printf("[getValueOfAttrByName] position of the attribute: %d\n", pos);
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

    int v;
    // memcpy(&v, (char *)value, sizeof(int));
    // printf("[getValueOfAttrByName]return value %s: %-10d\n", attrs[pos].name.c_str(), value);
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
