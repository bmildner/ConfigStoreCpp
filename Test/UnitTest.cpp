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

#define CONFIGURATION_UNITTEST_ENABLE_PRIVATEACCESS
#include "Configuration/Configuration.h"

using namespace std;
using namespace Configuration;


namespace Configuration
{
  namespace UnitTest
  {
    namespace Detail
    {
      // this is a somewhat dirty trick to get access to private members in Store objects ...
      struct PrivateAccess
      {
        static bool IsValidNewDelimiter(const Store& store, Store::String::value_type delimiter)
        {
          return store.IsValidNewDelimiter(delimiter);
        }

        static void SetNewDelimiter(Store& store, Store::String::value_type delimiter)
        {
          store.SetNewDelimiter(delimiter);
        }
      };
    }
  }
}

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

    catch (const ExceptionImpl<Exception>& e)
    {
      // check for exact exception type match!
      if (typeid(e) == typeid(ExceptionImpl<Exception>))
      {
        result = true;
      }
      else
      {
        exceptionInfo = ExceptionToString();
      }
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

  UniqueStorePtr CreateEmptyStore(const wstring& fileName = DefaultDatabaseFileName, Store::String::value_type delimiter = Store::DefaultNameDelimiter)
  {
    // make sure we really create a empty database by deleting the file if it exists
    boost::filesystem::remove(fileName);

    // instantiate function as local variable to avoid memory leak in case ctor throws!
    UniqueStorePtrDeleter func = [](Store* ptr) { ptr->CheckDataConsistency(); delete ptr; };

    return UniqueStorePtr(new Store(fileName, true, delimiter), move(func));
  }


  // generators
  namespace Detail
  {
    const Store::String RandomNameCharacterSetTemplate = L".!\"#$%&'()*+,-/0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~������߀";
  }

  Store::String GetRandomNameCharacterSet(Store::String::value_type delimiter)
  {
    Store::String randomNameCharacterSet = Detail::RandomNameCharacterSetTemplate;

    // <arg> please note that "erase(randomNameCharacterSet.find(delimiter));" is sadly a really bad idea here, almost looks linke an anomaly in the C++ standard library ...
    randomNameCharacterSet.erase(std::find(begin(randomNameCharacterSet), end(randomNameCharacterSet), delimiter));

    return randomNameCharacterSet;
  }

  size_t GetRandomNumber(size_t min = numeric_limits<size_t>::min(), size_t max = numeric_limits<size_t>::max())
  {
    // TODO: maybe add infrastucture to select between fixed and random seed for random test data
    // using a fixed seed inorder to have reproduceable results, at least for the time beeing ...
    static boost::random::mt19937 gen(4711);

    assert(min < max);

    return boost::random::uniform_int_distribution<size_t>(min, max)(gen);
  }

  namespace Detail
  {
    const size_t DefaultMaxRandomNameLength = 16;
    const size_t DefaultMinRandomNameLength = 1;
  }

  Store::String GenerateRandomName(size_t maxLen = Detail::DefaultMaxRandomNameLength, size_t minLen = Detail::DefaultMinRandomNameLength, Store::String::value_type delimiter = Store::DefaultNameDelimiter)
  {
    Store::String str;
    Store::String randomNameCharacterSet = GetRandomNameCharacterSet(delimiter);

    str.resize(GetRandomNumber(minLen, maxLen));

    for (auto& chr : str)
    {
      chr = randomNameCharacterSet.at(GetRandomNumber(0, randomNameCharacterSet.size() - 1));
    }
    
    assert(str.size() == wcslen(str.data()));

    return str;
  }

  Store::String GenerateRandomString(size_t maxLen, size_t minLen)
  {
    Store::String str;

    str.resize(GetRandomNumber(minLen, maxLen));

    for (auto& chr : str)
    {
      chr = Detail::RandomNameCharacterSetTemplate.at(GetRandomNumber(0, Detail::RandomNameCharacterSetTemplate.size() - 1));
    }

    assert(str.size() == wcslen(str.data()));

    return str;
  }

  Store::String GenerateRandomName(Store::String::value_type delimiter)
  {
    return GenerateRandomName(Detail::DefaultMaxRandomNameLength, Detail::DefaultMinRandomNameLength, delimiter);
  }

  // unit tests

  // TODO: add test for Store c'tor

  void TestIsValidName()
  { 
    // test with default path delimiter
    {
      auto store = CreateEmptyStore();

      ReadOnlyTransaction transaction(*store);  // check there is no writeable transaction in the implementation

      // check for const correctness
      static_cast<const Store&>(*store).IsValidName(L"");

      UNITTEST_ASSERT(store->GetNameDelimiter() == Store::DefaultNameDelimiter);
      UNITTEST_ASSERT(Store::DefaultNameDelimiter == L'.');

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
      UNITTEST_ASSERT(!store->IsValidName(L".1.2"));
      UNITTEST_ASSERT(!store->IsValidName(L"1.2."));
      UNITTEST_ASSERT(!store->IsValidName(L"..1.2"));
      UNITTEST_ASSERT(!store->IsValidName(L"1.2.."));
      UNITTEST_ASSERT(!store->IsValidName(L"1..2"));
      UNITTEST_ASSERT(!store->IsValidName(L"1...2"));

      UNITTEST_ASSERT(store->IsValidName(L"name"));
      UNITTEST_ASSERT(store->IsValidName(L"name.name"));
      UNITTEST_ASSERT(store->IsValidName(L"name.name.name"));
      UNITTEST_ASSERT(store->IsValidName(L"1"));
      UNITTEST_ASSERT(store->IsValidName(L"2.2"));
      UNITTEST_ASSERT(store->IsValidName(L"1.3.1"));

      UNITTEST_ASSERT(store->IsValidName(GetRandomNameCharacterSet(store->GetNameDelimiter())));

      for (size_t i = 0; i < 100; i++)
      {
        UNITTEST_ASSERT(store->IsValidName(GenerateRandomName()));
        UNITTEST_ASSERT(store->IsValidName(GenerateRandomName() + store->GetNameDelimiter() +
                                           GenerateRandomName()));
        UNITTEST_ASSERT(store->IsValidName(GenerateRandomName() + store->GetNameDelimiter() + 
                                           GenerateRandomName() + store->GetNameDelimiter() + 
                                           GenerateRandomName()));
      }
    }

    // test non-default path delimiter
    {
      // we try each character in our test character set as a delimiter!
      static const Store::String TestDelimiter = Detail::RandomNameCharacterSetTemplate;

      // create store with default delimeter and switch it as we go!      
      auto store = CreateEmptyStore(DefaultDatabaseFileName);

      // this should save a tremendous amount of IO for this test!
      WriteableTransaction transaction(*store);

      for (auto delimiter : TestDelimiter)
      {
		// set new delimeter through private access helper
        UNITTEST_ASSERT(Configuration::UnitTest::Detail::PrivateAccess::IsValidNewDelimiter(*store, delimiter));
        UNITTEST_ASSERT_NO_EXCEPTION(Configuration::UnitTest::Detail::PrivateAccess::SetNewDelimiter(*store, delimiter));

        UNITTEST_ASSERT(store->GetNameDelimiter() == delimiter);

        UNITTEST_ASSERT(!store->IsValidName(L""));

        UNITTEST_ASSERT(!store->IsValidName({delimiter}));
        UNITTEST_ASSERT(!store->IsValidName({delimiter, delimiter}));
        UNITTEST_ASSERT(!store->IsValidName({delimiter, delimiter, delimiter}));

        const Store::String delimStr(1, delimiter);

        UNITTEST_ASSERT(store->IsValidName(GetRandomNameCharacterSet(store->GetNameDelimiter())));

        for (size_t i = 0; i < 100; i++)
        {
          UNITTEST_ASSERT(store->IsValidName(GenerateRandomName(delimiter)));
          UNITTEST_ASSERT(store->IsValidName(GenerateRandomName(delimiter) + store->GetNameDelimiter() +
                                             GenerateRandomName(delimiter)));
          UNITTEST_ASSERT(store->IsValidName(GenerateRandomName(delimiter) + store->GetNameDelimiter() +
                                             GenerateRandomName(delimiter) + store->GetNameDelimiter() + 
                                             GenerateRandomName(delimiter)));
        }
      }

      transaction.Commit();
    }
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

    // check there is no writeable transaction in the implementation
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

    // check there is no writeable transaction in the implementation
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

    // check there is no writeable transaction in the implementation
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
    auto changed = [](const TrackedRevison& rev) -> bool { return rev.first != rev.second; };
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
    UNITTEST_ASSERT(!changed(rootRev));
    UNITTEST_ASSERT_THROWS(rootRev.second = store->GetRevision(L"Name1"), EntryNotFound);

    store->Create(L"Name1", -1);

    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
    reset(rootRev);
    UNITTEST_ASSERT(store->GetRevision() == store->GetRevision(L""));

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
    UNITTEST_ASSERT(!changed(name1Rev));
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(!changed(rootRev));

    store->Set(L"Name1", 1000);

    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(changed(name1Rev));
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
    reset(rootRev);

    store->Set(L"Name1", L"empty");

    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(changed(name1Rev));
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
    reset(rootRev);

    store->SetOrCreate(L"Name1", Store::Binary(4, 0x10));

    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(changed(name1Rev));
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
    reset(rootRev);
    UNITTEST_ASSERT_THROWS(rootRev.second = store->GetRevision(L"Name1.Name2"), EntryNotFound);

    store->Create(L"Name1.Name2", 0);

    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
    reset(rootRev);

    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(changed(name1Rev));
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
    UNITTEST_ASSERT(!changed(name2Rev));
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(!changed(name1Rev));
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(!changed(rootRev));

    store->SetOrCreate(L"Name1.Name2", 1000);

    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(changed(name2Rev));
    reset(name2Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(changed(name1Rev));
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
    reset(rootRev);

    store->Set(L"Name1.Name2", L"empty");

    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(changed(name2Rev));
    reset(name2Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(changed(name1Rev));
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
    reset(rootRev);

    store->Set(L"Name1.Name2", Store::Binary(4, 0x10));

    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(changed(name2Rev));
    reset(name2Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(changed(name1Rev));
    reset(name1Rev);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
    reset(rootRev);
    UNITTEST_ASSERT_THROWS(rootRev.second = store->GetRevision(L"Name3"), EntryNotFound);

    store->SetOrCreate(L"Name3", 4711);

    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(!changed(name2Rev));
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(!changed(name1Rev));
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
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
    UNITTEST_ASSERT(!changed(name3Rev));
    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(!changed(name2Rev));
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(!changed(name1Rev));
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(!changed(rootRev));

    // check there is no writeable transaction in the implementation
    {
      ReadOnlyTransaction transaction(*store);

      UNITTEST_ASSERT_NO_EXCEPTION(store->GetRevision());
      UNITTEST_ASSERT_NO_EXCEPTION(store->GetRevision(L"Name1"));
      UNITTEST_ASSERT_THROWS(store->GetRevision(L"NameX"), EntryNotFound);
      UNITTEST_ASSERT_THROWS(store->GetRevision(L"."), InvalidName);
    }

    // TODO: more tests with Delete and TryDelete needed?

    store->TryDelete(L"Name3", false);

    UNITTEST_ASSERT_THROWS(name3Rev.second = store->GetRevision(L"Name3"), EntryNotFound);
    UNITTEST_ASSERT_NO_EXCEPTION(name2Rev.second = store->GetRevision(L"Name1.Name2"));
    UNITTEST_ASSERT(!changed(name2Rev));
    UNITTEST_ASSERT_NO_EXCEPTION(name1Rev.second = store->GetRevision(L"Name1"));
    UNITTEST_ASSERT(!changed(name1Rev));
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
    reset(rootRev);

    store->Delete(L"Name1");

    UNITTEST_ASSERT_THROWS(name3Rev.second = store->GetRevision(L"Name1.Name2"), EntryNotFound);
    UNITTEST_ASSERT_THROWS(name3Rev.second = store->GetRevision(L"Name1"), EntryNotFound);
    UNITTEST_ASSERT_NO_EXCEPTION(rootRev.second = store->GetRevision());
    UNITTEST_ASSERT(changed(rootRev));
    reset(rootRev);
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

    set<Store::String>     names;
    vector<Store::String>  stringValues;
    vector<Store::Integer> intValues;

    stringValues.reserve(count);
    intValues.resize(count);

    for (size_t i = 0; i < count; i++)
    {
      while (!names.insert(GenerateRandomName()).second);  // names have to be unique
      stringValues.push_back(GenerateRandomString(35, 5));
      intValues[i] = GetRandomNumber();
    }

    cout << "Creating " << count << " entries:\n";

    auto store = CreateEmptyStore();
    WriteableTransaction transaction(*store);

    {      
      boost::timer::auto_cpu_timer timer;

      assert(names.size() == count);
      assert(names.size() == stringValues.size());
      assert(intValues.size() == stringValues.size());

      // TODO: is there a better way to iterate over multiple containers at once?
      size_t index = 0;

      for (const auto& name : names)
      {
        if (GetRandomNumber(0, 1) == 0)
        {
          store->Create(name, intValues[index]);
        }
        else
        {
          store->Create(name, stringValues[index]);
        }

        index++;
      }

      transaction.Commit();
    }

    cout << "\nTotal:\n";
  }
}  // anonymous namespace

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

#ifdef NDEBUG  // running the benchmark in debug mode makes no sense
      REGISTER_UNIT_TEST(Benchmark);
#endif      

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

