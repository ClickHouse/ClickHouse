#include <boost/dll/import.hpp>
#include <iostream>
#include <vector>

typedef void ModelCalcerHandle;


int main(int argc, char ** argv)
{

    if (argc != 2)
    {
        std::cout << "Usage: " << argv[0] << " catboost_lib_path model_path" << std::endl;
    }

    std::string lib_path = argv[1];
    std::string model_path = argv[2];

    std::cout << "Catboost lib path: " << lib_path << std::endl;
    std::cout << "Model path: " << std::endl;

    std::vector<int> v = {3, 4, 5};
    std::vector<int> vv = v;
    for (auto num : v)
        std::cout << num << std::endl;

    boost::dll::shared_library lib(lib_path);
    auto creater = lib.get<ModelCalcerHandle *()>("ModelCalcerCreate");
    auto destroyer = lib.get<void(ModelCalcerHandle *)>("ModelCalcerDelete");
    auto loader = lib.get<bool(ModelCalcerHandle *, const char*)>("LoadFullModelFromFile");

    std::cout << "Loaded lib" << std::endl;

    auto model = creater();
    std::cout << "created model" << std::endl;
    loader(model, model_path);
    std::cout << "loaded model" << std::endl;
    destroyer(model.c_str());
    std::cout << "destroyed model" << std::endl;
    return 0;
}
