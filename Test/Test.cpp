// Copyright (c) 2014 by Bertolt Mildner
// All rights reserved.

#include <iostream>

#include "SQLiteCpp\SQLiteCpp.h"

#include "Configuration\Configuration.h"

#include "UnitTest.h"

#include "Configuration\SortedVector.h"

enum class Type {Path, Integer, String, Binary, Object};


using namespace std;

template class Configuration::Detail::sorted_vector<int>;
template class Configuration::Detail::sorted_vector<wstring>;

int main()
{
  using Configuration::Detail::sorted_vector;

  sorted_vector<int> intVecSorted;
  sorted_vector<int> intVecSorted2;

  swap(intVecSorted, intVecSorted2);

  sorted_vector<wstring> strVecSorted;

  strVecSorted.emplace_back(L"");

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

    if (!store.Exists(L"Int"))
    {
      store.Create(L"Int", 4711);
    }
    else
    {
      store.GetInteger(L"Int");
    }

    if (!store.Exists(L"Str"))
    {
      store.Create(L"Str", L"value");
    }
    else
    {
      store.GetString(L"Str");
    }

    if (!store.Exists(L"Bin"))
    {
      store.Create(L"Bin", Configuration::Store::Binary(16, 0xcd));
    }
    else
    {
      store.GetBinary(L"Bin");
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