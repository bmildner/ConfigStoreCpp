#include <iostream>

#include "SQLiteCpp\SQLiteCpp.h"

#include "Configuration\Configuration.h"

#include "UnitTest.h"

enum class Type {Path, Integer, String, Binary, Object};


using namespace std;

int main()
{
  try
  {
    Configuration::Store store(L"Config.dat", true);

    store.CheckDataConsistency();

    {
      Configuration::WriteableTransaction trans(store);

      store.Exists(L"part1.part2.part3");

      if (store.Exists(L"hallo"))
      {
        store.Set(L"hallo", L"");
      }
      else
      {
        store.Create(L"hallo", L"");
      }

      store.SetOrCreate(L"hallo", L"lala");

      store.Set(L"hallo", L"world");

      store.HasChild(L"hallo");

      trans.Commit();
    }    

    Configuration::Store::Revision rev = store.GetRevision();

    if (rev != store.GetRevision())
    {
      rev = store.GetRevision(L"");

      return 1;
    }
  }

  catch (const Configuration::Exception& e)
  {
    wcout << "Exception: type (" << e.TypeName() << ") msg (" << e.What() << ")" << endl;
  }
  catch (const std::exception& e)
  {
    cout << "Exception: " << typeid(e).name() << " (" << e.what() << ")" << endl;
  }
  catch (...)
  {
    cout << "Unknown exception" << endl;
  }

 
  return Configuration::UnitTest::Run();
}