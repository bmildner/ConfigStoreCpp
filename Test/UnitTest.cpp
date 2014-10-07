// Copyright (c) 2014 by Bertolt Mildner
// All rights reserved.

#include "UnitTest.h"

#include <string>
#include <iostream>
#include <memory>
#include <functional>
#include <set>

#include "Configuration/Utils.h"

CONFIGURATION_BOOST_INCL_GUARD_BEGIN
#include <boost/filesystem/operations.hpp>
#include <boost/format.hpp>
#include <boost/timer/timer.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
CONFIGURATION_BOOST_INCL_GUARD_END

#include "Configuration/Configuration.h"

using namespace std;
using namespace Configuration;

namespace
{    
  const wstring DefaultDatabaseFileName = L"unittest.db";

  struct FailedUnittestAssertion : RuntimeError {};

  namespace Detail
  {
    wstring ExceptionToString(const FailedUnittestAssertion& e)
    {
      return e.What();
    }

    wstring ExceptionToString(const Configuration::Exception& e)
    {
      return (boost::wformat(L"exception type: %1%\n\nwhat: %2%") % e.TypeName() % e.What()).str();
    }

    wstring ExceptionToString(const exception& e)
    {
      return (boost::wformat(L"exception type: %1%\n\nwhat: %2%") % NarrowToWideStr(typeid(e).name()) % NarrowToWideStr(e.what())).str();
    }

    wstring UnknownExceptionToString()
    {
      return L"exception type: <unknown type>\n\nwhat: unknown exception caught";
    }
  }

  wstring ExceptionToString()
  {
    try
    {
      throw;
    }

    catch (const FailedUnittestAssertion& e)
    {
      return Detail::ExceptionToString(e);
    }

    catch (const Configuration::Exception& e)
    {
      return Detail::ExceptionToString(e);
    }

    catch (const std::exception& e)
    {
      return Detail::ExceptionToString(e);
    }

    catch (...)
    {
      return Detail::UnknownExceptionToString();
    }
  }

// throws FailedUnittestAssertion exception if expression x does not evaluate to true or throws an exception
#define UNITTEST_ASSERT(x) ConditionTest([&]() -> bool { return x; }, #x, __FILE__, __LINE__, __FUNCTION__)

  void ConditionTest(std::function<bool()> func, const char* functionText, const char* fileName, size_t lineNumber, const char* functionName)
  {
    bool result = false;
    wstring exceptionInfo;

    try
    {
      if (func())
      {
        result = true;
      }
      else
      {
        exceptionInfo = L"expression is false";
      }
    }

    catch (...)
    {
      exceptionInfo = ExceptionToString();
    }

    if (!result)
    {
      throw ExceptionImpl<FailedUnittestAssertion>((boost::wformat(L"UNITTEST_ASSERT(%1%)\nin %2%#%3% %4%\nfailed with %5%")
                                                                     % functionText % fileName % lineNumber % functionName % exceptionInfo).str());
    }
  }

// throws FailedUnittestAssertion exception if expression x throws an exception
#define UNITTEST_ASSERT_NO_EXCEPTION(x) NoExceptionTest([&]() { x; }, #x, __FILE__, __LINE__, __FUNCTION__)

  void NoExceptionTest(std::function<void()> func, const char* functionText, const char* fileName, size_t lineNumber, const char* functionName)
  {
    try
    {
      func();
    }

    catch (...)
    {
      throw ExceptionImpl<FailedUnittestAssertion>((boost::wformat(L"UNITTEST_ASSERT_NO_EXCEPTION(%1%)\nin %2%#%3% %4%\nfailed with %5%")
                                                                     % functionText % fileName % lineNumber % functionName % ExceptionToString()).str());
    }
  }

// throws exception if expression x does not throw an exception of exactly type Configuration::ExceptionImpl<ExceptionType>
#define UNITTEST_ASSERT_THROWS(x, ExceptionType) ExceptionTest<ExceptionType>([&]() { x; }, #x, #ExceptionType, __FILE__, __LINE__, __FUNCTION__)

  template <typename Exception>
  void ExceptionTest(std::function<void()> func, const char* functionText, const char* exceptionText, const char* fileName, size_t lineNumber, const char* functionName)
  {
    bool result = false;
    wstring exceptionInfo;

    try
    {
      func();
      exceptionInfo = L"out any exception beeing caught";
    }

    catch (const ExceptionImpl<Exception>&)
    {
      result = true;
    }

    catch (...)
    {
      exceptionInfo = ExceptionToString();
    }

    if (!result)
    {
      throw ExceptionImpl<FailedUnittestAssertion>((boost::wformat(L"UNITTEST_ASSERT_THROWS(%1%, %2%)\nin %3%#%4% %5%\nfailed with %6%") 
                                                                     % functionText % exceptionText % fileName % lineNumber % functionName % exceptionInfo).str());
    }
  }

  using UniqueStorePtrDeleter = function<void(Store* ptr)>;
  using UniqueStorePtr        = unique_ptr<Store, UniqueStorePtrDeleter>;

  UniqueStorePtr CreateEmptyStore(const wstring& fileName = DefaultDatabaseFileName)
  {
    // make sure we really create a empty database by deleting the file if it exists
    boost::filesystem::remove(fileName);

    // instantiate function as local variable to avoid memory leak in case ctor throws!
    UniqueStorePtrDeleter func = [](Store* ptr) { ptr->CheckDataConsistency(); delete ptr; };

    return UniqueStorePtr(new Store(fileName, true), move(func));
  }


  // generators
  const wstring RandomNameCharacterSet = L"!\"#$%&'()*+,-/0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~öäüÖÄÜß€";

  size_t GetRandomNumber(size_t min, size_t max)
  {
    static boost::random::mt19937 gen(static_cast<uint32_t>(time(0)));

    assert(min < max);

    return boost::random::uniform_int_distribution<>(min, max)(gen);
  }

  Store::String GenerateRandomName(size_t maxLen = 16, size_t minLen = 1)
  {
    Store::String str;

    str.resize(GetRandomNumber(minLen, maxLen));

    for (auto& chr : str)
    {
      chr = RandomNameCharacterSet.at(GetRandomNumber(0, RandomNameCharacterSet.size() - 1));
    }

    assert(str.size() == wcslen(str.data()));

    return str;
  }


  // unit tests

  void TestIsValidName()
  { 
    // TODO: test non-default path delimeter!
    auto store = CreateEmptyStore();

    ReadOnlyTransaction transaction(*store);  // check there is no writeable transaction in implementation

    // check for const correctness
    static_cast<const Store&>(*store).IsValidName(L"");

    UNITTEST_ASSERT(store->GetNameDelimeter() == Store::DefaultNameDelimeter);
    UNITTEST_ASSERT(Store::DefaultNameDelimeter == L'.');

    UNITTEST_ASSERT(!store->IsValidName(L""));
    UNITTEST_ASSERT(!store->IsValidName(L"."));
    UNITTEST_ASSERT(!store->IsValidName(L".."));
    UNITTEST_ASSERT(!store->IsValidName(L"..."));
    UNITTEST_ASSERT(!store->IsValidName(L".name1.name2"));
    UNITTEST_ASSERT(!store->IsValidName(L"name1.name2."));
    UNITTEST_ASSERT(!store->IsValidName(L"..name1.name2"));
    UNITTEST_ASSERT(!store->IsValidName(L"name1.name2.."));
    UNITTEST_ASSERT(!store->IsValidName(L"name1..name2"));
    UNITTEST_ASSERT(!store->IsValidName(L"name1...name2"));

    UNITTEST_ASSERT(store->IsValidName(L"name"));
    UNITTEST_ASSERT(store->IsValidName(L"name.name"));
    UNITTEST_ASSERT(store->IsValidName(L"name.name.name"));
    UNITTEST_ASSERT(store->IsValidName(RandomNameCharacterSet));
  }

  void TestExists()
  {
    auto store = CreateEmptyStore();

    // check for const correctness
    static_cast<const Store&>(*store).Exists(L"name");

    // check for name validation
    UNITTEST_ASSERT_THROWS(store->Exists(L""), InvalidName);
    
    UNITTEST_ASSERT(!store->Exists(L"name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name.name"));

    store->Create(L"name", 4711);

    UNITTEST_ASSERT(store->Exists(L"name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name.name"));

    store->Create(L"name.name", L"value");

    UNITTEST_ASSERT(store->Exists(L"name"));
    UNITTEST_ASSERT(store->Exists(L"name.name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name.name"));

    store->Create(L"name.name.name", -1);

    UNITTEST_ASSERT(store->Exists(L"name"));
    UNITTEST_ASSERT(store->Exists(L"name.name"));
    UNITTEST_ASSERT(store->Exists(L"name.name.name"));

    store->Delete(L"name.name");

    UNITTEST_ASSERT(store->Exists(L"name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name.name"));

    store->Delete(L"name", false);

    UNITTEST_ASSERT(!store->Exists(L"name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name.name"));

    store->Create(L"name.name.name", -1);

    UNITTEST_ASSERT(store->Exists(L"name"));
    UNITTEST_ASSERT(store->Exists(L"name.name"));
    UNITTEST_ASSERT(store->Exists(L"name.name.name"));

    // check for writeable transaction in implementation
    {
      ReadOnlyTransaction transaction(*store);

      UNITTEST_ASSERT(store->Exists(L"name"));
      UNITTEST_ASSERT(!store->Exists(L"name.notthere"));
      UNITTEST_ASSERT_THROWS(store->Exists(L"..."), InvalidName);
    }

    store->Delete(L"name");

    UNITTEST_ASSERT(!store->Exists(L"name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name"));
    UNITTEST_ASSERT(!store->Exists(L"name.name.name"));

    // test cases sensitivity
    store->Create(L"NAME", 0);

    UNITTEST_ASSERT(store->Exists(L"NAME"));
    UNITTEST_ASSERT(!store->Exists(L"Name"));
    UNITTEST_ASSERT(!store->Exists(L"name"));    

    store->Create(L"Name", 0);

    UNITTEST_ASSERT(store->Exists(L"NAME"));
    UNITTEST_ASSERT(store->Exists(L"Name"));
    UNITTEST_ASSERT(!store->Exists(L"name"));
  }

  void TestGetType()
  {
    auto store = CreateEmptyStore();

    // check for name validation
    UNITTEST_ASSERT_THROWS(store->GetType(L""), InvalidName);
    UNITTEST_ASSERT_THROWS(store->IsInteger(L""), InvalidName);
    UNITTEST_ASSERT_THROWS(store->IsString(L""), InvalidName);
    UNITTEST_ASSERT_THROWS(store->IsBinary(L""), InvalidName);

    // combined check for entry not found + const corectness
    UNITTEST_ASSERT_THROWS(static_cast<const Store&>(*store).GetType(L"name"), EntryNotFound);
    UNITTEST_ASSERT_THROWS(static_cast<const Store&>(*store).IsInteger(L"name"), EntryNotFound);
    UNITTEST_ASSERT_THROWS(static_cast<const Store&>(*store).IsString(L"name"), EntryNotFound);
    UNITTEST_ASSERT_THROWS(static_cast<const Store&>(*store).IsBinary(L"name"), EntryNotFound);

    store->Create(L"TypeTest.Integer", -1);

    UNITTEST_ASSERT(store->GetType(L"TypeTest.Integer") == Store::ValueType::Integer);
    UNITTEST_ASSERT(store->IsInteger(L"TypeTest.Integer"));
    UNITTEST_ASSERT(!store->IsString(L"TypeTest.Integer"));
    UNITTEST_ASSERT(!store->IsBinary(L"TypeTest.Integer"));

    store->Create(L"TypeTest.String", L"value");

    UNITTEST_ASSERT(store->GetType(L"TypeTest.String") == Store::ValueType::String);
    UNITTEST_ASSERT(!store->IsInteger(L"TypeTest.String"));
    UNITTEST_ASSERT(store->IsString(L"TypeTest.String"));
    UNITTEST_ASSERT(!store->IsBinary(L"TypeTest.String"));

    store->Create(L"TypeTest.Binary", Store::Binary(0xcd, 32));

    UNITTEST_ASSERT(store->GetType(L"TypeTest.Binary") == Store::ValueType::Binary);
    UNITTEST_ASSERT(!store->IsInteger(L"TypeTest.Binary"));
    UNITTEST_ASSERT(!store->IsString(L"TypeTest.Binary"));
    UNITTEST_ASSERT(store->IsBinary(L"TypeTest.Binary"));

    // test intermediate entry
    UNITTEST_ASSERT(store->GetType(L"TypeTest") == Store::ValueType::Integer);
    UNITTEST_ASSERT(store->IsInteger(L"TypeTest"));
    UNITTEST_ASSERT(!store->IsString(L"TypeTest"));
    UNITTEST_ASSERT(!store->IsBinary(L"TypeTest"));

    // check for writeable transaction in implementation
    {
      ReadOnlyTransaction transaction(*store);

      UNITTEST_ASSERT(store->GetType(L"TypeTest") == Store::ValueType::Integer);
      UNITTEST_ASSERT(store->IsInteger(L"TypeTest"));
      UNITTEST_ASSERT(!store->IsString(L"TypeTest"));
      UNITTEST_ASSERT(!store->IsBinary(L"TypeTest"));
      UNITTEST_ASSERT_THROWS(store->GetType(L"name"), EntryNotFound);
      UNITTEST_ASSERT_THROWS(store->IsInteger(L"name"), EntryNotFound);
      UNITTEST_ASSERT_THROWS(store->IsString(L"name"), EntryNotFound);
      UNITTEST_ASSERT_THROWS(store->IsBinary(L"name"), EntryNotFound);
    }

    store->Set(L"TypeTest", L"");

    UNITTEST_ASSERT(store->GetType(L"TypeTest") == Store::ValueType::String);
    UNITTEST_ASSERT(!store->IsInteger(L"TypeTest"));
    UNITTEST_ASSERT(store->IsString(L"TypeTest"));
    UNITTEST_ASSERT(!store->IsBinary(L"TypeTest"));

    store->Set(L"TypeTest", Store::Binary());

    store->GetBinary(L"TypeTest");

    UNITTEST_ASSERT(store->GetType(L"TypeTest") == Store::ValueType::Binary);
    UNITTEST_ASSERT(!store->IsInteger(L"TypeTest"));
    UNITTEST_ASSERT(!store->IsString(L"TypeTest"));
    UNITTEST_ASSERT(store->IsBinary(L"TypeTest"));
  }

  void TestHasChild()
  {
    auto store = CreateEmptyStore();

    // check for name validation + const correctnes
    UNITTEST_ASSERT_THROWS(static_cast<const Store&>(*store).HasChild(L"."), InvalidName);
    UNITTEST_ASSERT_THROWS(static_cast<const Store&>(*store).GetChildren(L"."), InvalidName);

    // empty name is allowed to signal root
    UNITTEST_ASSERT(!store->HasChild(L""));
    UNITTEST_ASSERT(store->GetChildren(L"").size() == 0);

    UNITTEST_ASSERT_THROWS(store->HasChild(L"name"), EntryNotFound);
    UNITTEST_ASSERT_THROWS(store->GetChildren(L"name"), EntryNotFound);

    store->Create(L"value1", 0);

    UNITTEST_ASSERT(store->HasChild(L""));
    UNITTEST_ASSERT(store->GetChildren(L"").size() == 1);
    UNITTEST_ASSERT(store->GetChildren(L"")[0] == L"value1");

    UNITTEST_ASSERT(!store->HasChild(L"value1"));
    UNITTEST_ASSERT(store->GetChildren(L"value1").size() == 0);

    store->Create(L"value2", 0);

    UNITTEST_ASSERT(store->HasChild(L""));
    UNITTEST_ASSERT(store->GetChildren(L"").size() == 2);
    UNITTEST_ASSERT(store->GetChildren(L"")[0] == L"value1");
    UNITTEST_ASSERT(store->GetChildren(L"")[1] == L"value2");

    UNITTEST_ASSERT(!store->HasChild(L"value1"));
    UNITTEST_ASSERT(store->GetChildren(L"value1").size() == 0);
    UNITTEST_ASSERT(!store->HasChild(L"value2"));
    UNITTEST_ASSERT(store->GetChildren(L"value2").size() == 0);

    store->Create(L"value2.value3", 0);

    UNITTEST_ASSERT(store->HasChild(L"value2"));
    UNITTEST_ASSERT(store->GetChildren(L"value2").size() == 1);
    UNITTEST_ASSERT(store->GetChildren(L"value2")[0] == L"value3");

    UNITTEST_ASSERT(store->HasChild(L""));
    UNITTEST_ASSERT(store->GetChildren(L"").size() == 2);
    UNITTEST_ASSERT(store->GetChildren(L"")[0] == L"value1");
    UNITTEST_ASSERT(store->GetChildren(L"")[1] == L"value2");

    UNITTEST_ASSERT(!store->HasChild(L"value1"));
    UNITTEST_ASSERT(store->GetChildren(L"value1").size() == 0);

    store->Delete(L"value2");

    // check for writeable transaction in implementation
    {
      ReadOnlyTransaction transaction(*store);

      UNITTEST_ASSERT_THROWS(store->HasChild(L"value2"), EntryNotFound);
      UNITTEST_ASSERT_THROWS(store->GetChildren(L"value2"), EntryNotFound);

      UNITTEST_ASSERT(store->HasChild(L""));
      UNITTEST_ASSERT(store->GetChildren(L"").size() == 1);
      UNITTEST_ASSERT(store->GetChildren(L"")[0] == L"value1");

      UNITTEST_ASSERT(!store->HasChild(L"value1"));
      UNITTEST_ASSERT(store->GetChildren(L"value1").size() == 0);
    }

    store->Delete(L"value1");

    UNITTEST_ASSERT_THROWS(store->HasChild(L"value2"), EntryNotFound);
    UNITTEST_ASSERT_THROWS(store->GetChildren(L"value2"), EntryNotFound);

    UNITTEST_ASSERT(!store->HasChild(L""));
    UNITTEST_ASSERT(store->GetChildren(L"").size() == 0);
  }

  void TestGetRevision()
  {
    auto store = CreateEmptyStore();

    using TrackedRevison = pair<Store::Revision, Store::Revision>;  // .first == old value, .second == new/currrent value; (.first == .second) => not changed
    auto reset = [](TrackedRevison& rev) { rev.first = rev.second; };

    // check for name validation (empty == root)
    UNITTEST_ASSERT_THROWS(store->GetRevision(L".."), InvalidName);

    TrackedRevison rootRev;

    // check for default param L"" + check for const correctness
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.first = static_cast<const Store&>(*store).GetRevision());
    store->HasChild(L"");
    store->GetChildren(L"");
    store->Exists(L"name");
    // check for L"" == root
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision(L""));
    UNITTEST_ASSERT(rootRev.first == rootRev.second);
    UNITTEST_ASSERT_THROWS(rootRev.second = store->GetRevision(L"Name1"), EntryNotFound);

    store->Create(L"Name1", -1);

    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first != rootRev.second);
    reset(rootRev);

    TrackedRevison name1Rev;

    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.first = store->GetRevision(L"Name1"));

    store->HasChild(L"Name1");
    store->GetChildren(L"Name1");
    store->Exists(L"Name1");
    store->Exists(L"NameX");
    store->Exists(L"Name1.NameX");
    store->TryDelete(L"Name1.NameX");
    store->GetType(L"Name1");
    store->IsInteger(L"Name1");
    store->IsString(L"Name1");
    store->IsBinary(L"Name1");

    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first == name1Rev.second);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first == rootRev.second);

    store->Set(L"Name1", 1000);

    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first != name1Rev.second);
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first != rootRev.second);
    reset(rootRev);

    store->Set(L"Name1", L"empty");

    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first != name1Rev.second);
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first != rootRev.second);
    reset(rootRev);

    store->SetOrCreate(L"Name1", Store::Binary(4, 0x10));

    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first != name1Rev.second);
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first != rootRev.second);
    reset(rootRev);
    UNITTEST_ASSERT_THROWS(rootRev.second = store->GetRevision(L"Name1.Name2"), EntryNotFound);

    store->Create(L"Name1.Name2", 0);

    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first != rootRev.second);
    reset(rootRev);

    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first != name1Rev.second);
    reset(name1Rev);

    TrackedRevison name2Rev;

    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.first = store->GetRevision(L"Name1.Name2"));

    store->HasChild(L"Name1");
    store->GetChildren(L"Name1");
    store->Exists(L"Name1");
    store->Exists(L"NameX");
    store->Exists(L"Name1.Name2");
    store->Exists(L"Name1.NameX");
    store->TryDelete(L"Name1.NameX");
    store->GetType(L"Name1");
    store->IsInteger(L"Name1");
    store->IsString(L"Name1");
    store->IsBinary(L"Name1");
    store->HasChild(L"Name1.Name2");
    store->GetChildren(L"Name1.Name2");
    store->Exists(L"Name1.Name2");
    store->Exists(L"Name1.Name2.NameX");
    store->TryDelete(L"Name1.Name2.NameX");
    store->GetType(L"Name1.Name2");
    store->IsInteger(L"Name1.Name2");
    store->IsString(L"Name1.Name2");
    store->IsBinary(L"Name1.Name2");

    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(name2Rev.first == name2Rev.second);
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first == name1Rev.second);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first == rootRev.second);

    store->SetOrCreate(L"Name1.Name2", 1000);

    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(name2Rev.first != name2Rev.second);
    reset(name2Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first != name1Rev.second);
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first != rootRev.second);
    reset(rootRev);

    store->Set(L"Name1.Name2", L"empty");

    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(name2Rev.first != name2Rev.second);
    reset(name2Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first != name1Rev.second);
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first != rootRev.second);
    reset(rootRev);

    store->Set(L"Name1.Name2", Store::Binary(4, 0x10));

    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(name2Rev.first != name2Rev.second);
    reset(name2Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first != name1Rev.second);
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first != rootRev.second);
    reset(rootRev);
    UNITTEST_ASSERT_THROWS(rootRev.second = store->GetRevision(L"Name3"), EntryNotFound);

    store->SetOrCreate(L"Name3", 4711);

    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(name2Rev.first == name2Rev.second);
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first == name1Rev.second);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first != rootRev.second);
    reset(rootRev);

    TrackedRevison name3Rev;

    UNITTEST_ASSERT_NO_EXCEPTION(name3Rev.first = store->GetRevision(L"Name3"));

    store->HasChild(L"Name1");
    store->GetChildren(L"Name1");
    store->Exists(L"Name1");
    store->Exists(L"NameX");
    store->Exists(L"Name1.Name2");
    store->Exists(L"Name1.NameX");
    store->TryDelete(L"Name1.NameX");
    store->GetType(L"Name1");
    store->IsInteger(L"Name1");
    store->IsString(L"Name1");
    store->IsBinary(L"Name1");
    store->HasChild(L"Name1.Name2");
    store->GetChildren(L"Name1.Name2");
    store->Exists(L"Name1.Name2");
    store->Exists(L"Name1.Name2.NameX");
    store->TryDelete(L"Name1.Name2.NameX");
    store->GetType(L"Name1.Name2");
    store->IsInteger(L"Name1.Name2");
    store->IsString(L"Name1.Name2");
    store->IsBinary(L"Name1.Name2");
    store->HasChild(L"Name3");
    store->GetChildren(L"Name3");
    store->Exists(L"Name3");
    store->Exists(L"Name3.NameX");
    store->TryDelete(L"Name3.NameX");
    store->GetType(L"Name3");
    store->IsInteger(L"Name3");
    store->IsString(L"Name3");
    store->IsBinary(L"Name3");

    UNITTEST_ASSERT_NO_EXCEPTION(name3Rev.second = store->GetRevision(L"Name3"));
    UNITTEST_ASSERT(name3Rev.first == name3Rev.second);
    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(name2Rev.first == name2Rev.second);
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(name1Rev.first == name1Rev.second);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(rootRev.first == rootRev.second);

    // TODO: delete + TryDelete
    // TODO: check for writeable transaction
  }

  void TestCreate()
  {
    auto store = CreateEmptyStore();

    // check for name validation 
    UNITTEST_ASSERT_THROWS(store->Create(L"", 0), InvalidName);
    
    UNITTEST_ASSERT_NO_EXCEPTION(store->Create(L"name", 0));
    UNITTEST_ASSERT(store->Exists(L"name"));
    UNITTEST_ASSERT(store->IsInteger(L"name"));
    UNITTEST_ASSERT(store->GetInteger(L"name") == 0);
    UNITTEST_ASSERT_THROWS(store->Create(L"name", 0), NameAlreadyExists);

    store->Delete(L"name");

    UNITTEST_ASSERT_NO_EXCEPTION(store->Create(L"name", L"value"));

    UNITTEST_ASSERT_NO_EXCEPTION(store->Create(L"Name", L"value"));
    UNITTEST_ASSERT(store->Exists(L"Name"));
    UNITTEST_ASSERT(store->IsString(L"Name"));
    UNITTEST_ASSERT(store->GetString(L"Name") == L"value");
    UNITTEST_ASSERT_THROWS(store->Create(L"Name", 0), NameAlreadyExists);

    UNITTEST_ASSERT_NO_EXCEPTION(store->Create(L"NAME", Store::Binary()));
    UNITTEST_ASSERT(store->Exists(L"NAME"));
    UNITTEST_ASSERT(store->IsBinary(L"NAME"));
    UNITTEST_ASSERT(store->GetBinary(L"NAME") == Store::Binary());
    UNITTEST_ASSERT_THROWS(store->Create(L"NAME", 0), NameAlreadyExists);    

    UNITTEST_ASSERT_NO_EXCEPTION(store->Create(L"name1.name2.name3", L"value"));
    UNITTEST_ASSERT(store->Exists(L"name1"));
    UNITTEST_ASSERT(store->IsInteger(L"name1"));
    UNITTEST_ASSERT(store->GetInteger(L"name1") == 0);
    UNITTEST_ASSERT(store->Exists(L"name1.name2"));
    UNITTEST_ASSERT(store->IsInteger(L"name1.name2"));
    UNITTEST_ASSERT(store->GetInteger(L"name1.name2") == 0);
    UNITTEST_ASSERT(store->Exists(L"name1.name2.name3"));
    UNITTEST_ASSERT(store->IsString(L"name1.name2.name3"));
    UNITTEST_ASSERT(store->GetString(L"name1.name2.name3") == L"value");
    UNITTEST_ASSERT_THROWS(store->Create(L"name1", 0), NameAlreadyExists);
    UNITTEST_ASSERT_THROWS(store->Create(L"name1.name2", 0), NameAlreadyExists);
    UNITTEST_ASSERT_THROWS(store->Create(L"name1.name2.name3", 0), NameAlreadyExists);

    store->Delete(L"name1");

    UNITTEST_ASSERT_NO_EXCEPTION(store->Create(L"name1.name2.name3", 4711));
    UNITTEST_ASSERT(store->Exists(L"name1"));
    UNITTEST_ASSERT(store->IsInteger(L"name1"));
    UNITTEST_ASSERT(store->GetInteger(L"name1") == 0);
    UNITTEST_ASSERT(store->Exists(L"name1.name2"));
    UNITTEST_ASSERT(store->IsInteger(L"name1.name2"));
    UNITTEST_ASSERT(store->GetInteger(L"name1.name2") == 0);
    UNITTEST_ASSERT(store->Exists(L"name1.name2.name3"));
    UNITTEST_ASSERT(store->IsInteger(L"name1.name2.name3"));
    UNITTEST_ASSERT(store->GetInteger(L"name1.name2.name3") == 4711);
    UNITTEST_ASSERT_THROWS(store->Create(L"name1", 0), NameAlreadyExists);
    UNITTEST_ASSERT_THROWS(store->Create(L"name1.name2", 0), NameAlreadyExists);
    UNITTEST_ASSERT_THROWS(store->Create(L"name1.name2.name3", 0), NameAlreadyExists);

    store->Delete(L"name1");

    UNITTEST_ASSERT_NO_EXCEPTION(store->Create(L"name1.name2.name3", Store::Binary(16, 0xff)));
    UNITTEST_ASSERT(store->Exists(L"name1"));
    UNITTEST_ASSERT(store->IsInteger(L"name1"));
    UNITTEST_ASSERT(store->GetInteger(L"name1") == 0);
    UNITTEST_ASSERT(store->Exists(L"name1.name2"));
    UNITTEST_ASSERT(store->IsInteger(L"name1.name2"));
    UNITTEST_ASSERT(store->GetInteger(L"name1.name2") == 0);
    UNITTEST_ASSERT(store->Exists(L"name1.name2.name3"));
    UNITTEST_ASSERT(store->IsBinary(L"name1.name2.name3"));
    UNITTEST_ASSERT(store->GetBinary(L"name1.name2.name3") == Store::Binary(16, 0xff));
    UNITTEST_ASSERT_THROWS(store->Create(L"name1", 0), NameAlreadyExists);
    UNITTEST_ASSERT_THROWS(store->Create(L"name1.name2", 0), NameAlreadyExists);
    UNITTEST_ASSERT_THROWS(store->Create(L"name1.name2.name3", 0), NameAlreadyExists);

    // check for writeable transaction in implementation
    {
      ReadOnlyTransaction transaction(*store);

      UNITTEST_ASSERT_THROWS(store->Create(L"name1", 0), InvalidTransaction);
      UNITTEST_ASSERT_THROWS(store->Create(L"name2", L""), InvalidTransaction);
      UNITTEST_ASSERT_THROWS(store->Create(L"name1.name2", Store::Binary()), InvalidTransaction);
    }
  }

  void TestSet()
  {
    auto store = CreateEmptyStore();

    // check for name validation 
    UNITTEST_ASSERT_THROWS(store->Set(L"", 0), InvalidName);

    UNITTEST_ASSERT_THROWS(store->Set(L"name1", 0), EntryNotFound);
    store->Create(L"name1", 0);
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1", 1));
    UNITTEST_ASSERT(store->IsInteger(L"name1"));
    UNITTEST_ASSERT(store->GetInteger(L"name1") == 1);
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1", L"val"));
    UNITTEST_ASSERT(store->IsString(L"name1"));
    UNITTEST_ASSERT(store->GetString(L"name1") == L"val");
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1", Store::Binary(8, 0xcd)));
    UNITTEST_ASSERT(store->IsBinary(L"name1"));
    UNITTEST_ASSERT(store->GetBinary(L"name1") == Store::Binary(8, 0xcd));
    store->Delete(L"name1");
    UNITTEST_ASSERT_THROWS(store->Set(L"name1", 0), EntryNotFound);

    UNITTEST_ASSERT_THROWS(store->Set(L"name1.name2", L"value"), EntryNotFound);
    store->Create(L"name1.name2", 0);
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1.name2", 1));    
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1", L"s"));
    UNITTEST_ASSERT(store->IsString(L"name1"));
    UNITTEST_ASSERT(store->GetString(L"name1") == L"s");
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1", 1));
    UNITTEST_ASSERT(store->IsInteger(L"name1"));
    UNITTEST_ASSERT(store->GetInteger(L"name1") == 1);
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1", Store::Binary(8, 0x80)));
    UNITTEST_ASSERT(store->IsBinary(L"name1"));
    UNITTEST_ASSERT(store->GetBinary(L"name1") == Store::Binary(8, 0x80));
    UNITTEST_ASSERT(store->IsInteger(L"name1.name2"));
    UNITTEST_ASSERT(store->GetInteger(L"name1.name2") == 1);
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1.name2", L"val"));
    UNITTEST_ASSERT(store->IsString(L"name1.name2"));
    UNITTEST_ASSERT(store->GetString(L"name1.name2") == L"val");
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1.name2", Store::Binary(8, 0xcd)));
    UNITTEST_ASSERT(store->IsBinary(L"name1.name2"));
    UNITTEST_ASSERT(store->GetBinary(L"name1.name2") == Store::Binary(8, 0xcd));
    store->Delete(L"name1.name2");
    UNITTEST_ASSERT_THROWS(store->Set(L"name1.name2", L"value"), EntryNotFound);

    UNITTEST_ASSERT_THROWS(store->Set(L"name1.name2.name3", Store::Binary(2, 0x11)), EntryNotFound);
    store->Create(L"name1.name2.name3", 0);
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1.name2.name3", 1));
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1.name2", L"s"));
    UNITTEST_ASSERT(store->IsString(L"name1.name2"));
    UNITTEST_ASSERT(store->GetString(L"name1.name2") == L"s");
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1.name2", 1));
    UNITTEST_ASSERT(store->IsInteger(L"name1.name2"));
    UNITTEST_ASSERT(store->GetInteger(L"name1.name2") == 1);
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1.name2", Store::Binary(8, 0x80)));
    UNITTEST_ASSERT(store->IsBinary(L"name1.name2"));
    UNITTEST_ASSERT(store->GetBinary(L"name1.name2") == Store::Binary(8, 0x80));
    UNITTEST_ASSERT(store->IsInteger(L"name1.name2.name3"));
    UNITTEST_ASSERT(store->GetInteger(L"name1.name2.name3") == 1);
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1.name2.name3", L"val"));
    UNITTEST_ASSERT(store->IsString(L"name1.name2.name3"));
    UNITTEST_ASSERT(store->GetString(L"name1.name2.name3") == L"val");
    UNITTEST_ASSERT_NO_EXCEPTION(store->Set(L"name1.name2.name3", Store::Binary(8, 0xcd)));
    UNITTEST_ASSERT(store->IsBinary(L"name1.name2.name3"));
    UNITTEST_ASSERT(store->GetBinary(L"name1.name2.name3") == Store::Binary(8, 0xcd));
    store->Delete(L"name1.name2.name3");
    UNITTEST_ASSERT_THROWS(store->Set(L"name1.name2.name3", Store::Binary(2, 0x11)), EntryNotFound);

    // check for writeable transaction in implementation
    {
      ReadOnlyTransaction transaction(*store);

      UNITTEST_ASSERT_THROWS(store->Set(L"name1", 0), InvalidTransaction);
      UNITTEST_ASSERT_THROWS(store->Set(L"name2", L""), InvalidTransaction);
      UNITTEST_ASSERT_THROWS(store->Set(L"name1.name2", Store::Binary(9, 0xef)), InvalidTransaction);
    }
  }

  void TestWriteableTransaction()
  {
    auto store = CreateEmptyStore();

    // check nested writeable transaction
    {
      store->Create(L"Test.Transaction.WriteableTransaction.trans1.1", 0);
      store->Create(L"Test.Transaction.WriteableTransaction.trans1.2", 0);
      store->Create(L"Test.Transaction.WriteableTransaction.trans2.1", 0);
      store->Create(L"Test.Transaction.WriteableTransaction.trans2.2", 0);
      store->Create(L"Test.Transaction.WriteableTransaction.trans3", 0);

      {
        // check we support move-construction
        Configuration::WriteableTransaction trans1 = WriteableTransaction(*store);

        store->Set(L"Test.Transaction.WriteableTransaction.trans1.1", 1);

        {
          Configuration::WriteableTransaction trans2(*store);

          store->Set(L"Test.Transaction.WriteableTransaction.trans2.1", 1);

          {
            Configuration::WriteableTransaction trans3(*store);

            store->Set(L"Test.Transaction.WriteableTransaction.trans3", 1);

            trans3.Commit();
          }

          store->Set(L"Test.Transaction.WriteableTransaction.trans2.2", 1);

          trans2.Commit();
        }

        store->Set(L"Test.Transaction.WriteableTransaction.trans1.2", 1);

        trans1.Commit();
      }

      // check resulting values, all set
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans1.1") == 1);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans1.2") == 1);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans2.1") == 1);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans2.2") == 1);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans3") == 1);
    }
    // check inner transaction rollback with nested writeable transaction
    {
      store->Set(L"Test.Transaction.WriteableTransaction.trans1.1", 0);
      store->Set(L"Test.Transaction.WriteableTransaction.trans1.2", 0);
      store->Set(L"Test.Transaction.WriteableTransaction.trans2.1", 0);
      store->Set(L"Test.Transaction.WriteableTransaction.trans2.2", 0);
      store->Set(L"Test.Transaction.WriteableTransaction.trans3", 0);

      {
        Configuration::WriteableTransaction trans1(*store);

        store->Set(L"Test.Transaction.WriteableTransaction.trans1.1", 1);

        {
          Configuration::WriteableTransaction trans2(*store);

          store->Set(L"Test.Transaction.WriteableTransaction.trans2.1", 1);

          {
            Configuration::WriteableTransaction trans3(*store);

            store->Set(L"Test.Transaction.WriteableTransaction.trans3", 1);

            // trans3.Commit();
          }

          store->Set(L"Test.Transaction.WriteableTransaction.trans2.2", 1);

          trans2.Commit();
        }

        store->Set(L"Test.Transaction.WriteableTransaction.trans1.2", 1);

        trans1.Commit();
      }

      // check resulting values, only inner transaction trans3 must have been rolled back
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans1.1") == 1);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans1.2") == 1);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans2.1") == 1);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans2.2") == 1);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans3") == 0);
    }

    // check outer transaction rollback with nested writeable transaction
    {
      store->SetOrCreate(L"Test.Transaction.WriteableTransaction.trans1.1", 0);
      store->SetOrCreate(L"Test.Transaction.WriteableTransaction.trans1.2", 0);
      store->SetOrCreate(L"Test.Transaction.WriteableTransaction.trans2.1", 0);
      store->SetOrCreate(L"Test.Transaction.WriteableTransaction.trans2.2", 0);
      store->SetOrCreate(L"Test.Transaction.WriteableTransaction.trans3", 0);

      {
        Configuration::WriteableTransaction trans1(*store);

        store->Set(L"Test.Transaction.WriteableTransaction.trans1.1", 1);

        {
          Configuration::WriteableTransaction trans2(*store);

          store->Set(L"Test.Transaction.WriteableTransaction.trans2.1", 1);

          {
            Configuration::WriteableTransaction trans3(*store);

            store->Set(L"Test.Transaction.WriteableTransaction.trans3", 1);

            trans3.Commit();
          }

          store->Set(L"Test.Transaction.WriteableTransaction.trans2.2", 1);

          trans2.Commit();
        }

        store->Set(L"Test.Transaction.WriteableTransaction.trans1.2", 1);

        //trans1.Commit();
      }

      // check resulting values, no changes
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans1.1") == 0);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans1.2") == 0);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans2.1") == 0);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans2.2") == 0);
      UNITTEST_ASSERT(store->GetInteger(L"Test.Transaction.WriteableTransaction.trans3") == 0);
    }

    // writeable transactions can not be nested with read only transactions
    {
      ReadOnlyTransaction transaction(*store);
     
      UNITTEST_ASSERT_THROWS(WriteableTransaction trans(*store), InvalidTransaction);
    }
  }

  void Benchmark()
  {
    static const size_t count = 10000;

    set<Store::String> names;

    for (size_t i = 0; i <= count; i++)
    {
      while (!names.insert(GenerateRandomName()).second);
    }

    cout << "Creating " << count << " entries:\n";

    auto store = CreateEmptyStore();
    WriteableTransaction transaction(*store);

    {
      boost::timer::auto_cpu_timer timer;

      for (const auto& name : names)
      {
        store->Create(name, 0);
      }

      transaction.Commit();
    }

    cout << "\nTotal:\n";
  }
}

namespace Configuration
{
  namespace UnitTest
  {
    bool Run()
    {
      boost::timer::auto_cpu_timer timer;
      bool result = true;
      vector<pair<function<void()>, string>>     tests;

#define REGISTER_UNIT_TEST(x) tests.emplace_back(make_pair(x, #x))

      REGISTER_UNIT_TEST(TestIsValidName);
      REGISTER_UNIT_TEST(TestExists);
      REGISTER_UNIT_TEST(TestGetType);
      REGISTER_UNIT_TEST(TestHasChild);
      REGISTER_UNIT_TEST(TestGetRevision);
      REGISTER_UNIT_TEST(TestCreate);
      REGISTER_UNIT_TEST(TestSet);

      REGISTER_UNIT_TEST(TestWriteableTransaction);
      
      REGISTER_UNIT_TEST(Benchmark);
      

      for (const auto& test : tests)
      {
        cout << "\nConfiguration::UnitTests::" << test.second << "():\n";

        try
        {
          boost::timer::auto_cpu_timer timer;
          test.first();
        }

        catch (...)
        {
          wcerr << L"\n" << ExceptionToString() << L"\n\n";
          result = false;
        }
      }

      wcout << L"\n\nConfiguration::UnitTests::Run():\n";

      return !result;
    }
  }
}

