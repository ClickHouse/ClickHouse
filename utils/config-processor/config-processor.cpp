#include <Common/Config/ConfigProcessor.h>
#include <iostream>

int main(int argc, char ** argv)
{
    try
    {
        if (argc != 2)
        {
            std::cerr << "usage: " << argv[0] << " path" << std::endl;
            return 3;
        }

        ConfigProcessor processor(argv[1], false, true);
        XMLDocumentPtr document = processor.processConfig();
        Poco::XML::DOMWriter().writeNode(std::cout, document);
    }
    catch (Poco::Exception & e)
    {
        std::cerr << "Exception: " << e.displayText() << std::endl;
        return 1;
    }
    catch (std::exception & e)
    {
        std::cerr << "std::exception: " << e.what() << std::endl;
        return 3;
    }
    catch (...)
    {
        std::cerr << "Some exception" << std::endl;
        return 2;
    }

    return 0;
}
