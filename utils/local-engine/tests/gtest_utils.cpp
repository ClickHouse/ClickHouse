#include <gtest/gtest.h>
#include <Common/StringUtils.h>

using namespace local_engine;

TEST(TestStringUtils, TestExtractPartitionValues)
{
    std::string path = "/tmp/col1=1/col2=test/a.parquet";
    auto values = StringUtils::parsePartitionTablePath(path);
    ASSERT_EQ(2, values.size());
    ASSERT_EQ("col1", values[0].first);
    ASSERT_EQ("1", values[0].second);
    ASSERT_EQ("col2", values[1].first);
    ASSERT_EQ("test", values[1].second);
}
