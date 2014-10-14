// Copyright (c) 2014 by Bertolt Mildner
// All rights reserved.

#ifndef CONFIGURATION_H
#define CONFIGURATION_H

#pragma once

#include <string>
#include <memory>
#include <cstdint>
#include <vector>
#include <exception>
#include <functional>
#include <map>

#include <boost\noncopyable.hpp>

#include "Utils.h"


// forward declarations of SQLiteCpp types we need
namespace SQLite
{
  class Statement;
  class Transaction;
  class Database;
}

namespace Configuration
{
  namespace Detail
  {
    class RandomNumberGenerator;
  }

  namespace UnitTest
  {
    namespace Detail
    {
      // this is a somewhat dirty trick to get access to private members in Store objects ...
      struct PrivateAccess;
    }
  }

  class ReadOnlyTransaction;
  class WriteableTransaction;

  // TODO: multi-thread safety !?!?!
  // TODO: retries in case of a busy database!?

  // TODO: add Clone() for (fast?) cloning of Store objects to be used in an other thread
  // TODO: add change notification using call-back
  // TODO: add possibility to open Store from readonly database file (file attribute + filesystem access control)
  // TODO: add overloads for public interface taking path+name (avoid string concatination, concatinate id path internally!)

  // not multi-thread safe due to limitation in SQLite!
  // create Store instance for each thread
  class Store : private boost::noncopyable
  {
    public:
      using Integer = std::int64_t;
      using String  = std::wstring;
      using Binary  = std::vector<std::uint8_t>;

      using Children = std::vector<String>;

      enum class ValueType {Integer = 1, String = 2, Binary = 3};

      struct Revision
      {
        inline Revision(Integer id = 0, Integer revision = 0) noexcept
        : m_Id(id), m_Revision(revision)
        {}

        inline bool operator==(const Revision& rhs) const noexcept { return (m_Id == rhs.m_Id) && (m_Revision == rhs.m_Revision); }
        inline bool operator!=(const Revision& rhs) const noexcept { return !(*this == rhs); }

        Integer m_Id;
        Integer m_Revision;
      };

      static const String::value_type DefaultNameDelimiter;

      // opens existing configuration store
      explicit Store(const std::wstring& fileName, bool create = false, wchar_t nameDelimiter = DefaultNameDelimiter);

      ~Store() noexcept;

      String::value_type GetNameDelimiter() const noexcept;

      // valid names must not:
      // - start or end with a delimiter
      // - conttain multiple consecutive delimiters
      // - be empty
      // Note: no Unicode normalization is done before comparison of names or writeing them into the database!
      inline bool IsValidName(const String& name) const
      {
        return IsValidName(name, m_Delimiter);
      }
      // only use if you really have to validate a name w/o an Store object in hand!
      static bool IsValidName(const String& name, String::value_type delimiter);

      bool Exists(const String& name) const;

      ValueType GetType(const String& name) const;
      bool IsString(const String& name) const;
      bool IsInteger(const String& name) const;      
      bool IsBinary(const String& name) const;

      // empty name == root, revision of the whole store
      // there are corner-cases where a change may not be detected:
      //   if the revision of the entry was bumped exactly 2^(sizeof(<internal revision representation>) * 8) times in the meantime
      //   if the entry has been deleted and re-created in the meantime AND the new entry happens to have the same id AND the same revision by accident
      // -> next to impossible in finite time and space given that entry ids and revisions internally are at least 64 bit wide
      Revision GetRevision(const String& name = L"") const;

      // empty name == root
      bool HasChild(const String& name) const;
      // empty name == root
      Children GetChildren(const String& name) const;

      // create new entry, fails if already exests
      void Create(const String& name, const String& value);
      void Create(const String& name, Integer       value);
      void Create(const String& name, const Binary& value);

      // set existing entry, fails if not already existing
      void Set(const String& name, const String& value);
      void Set(const String& name, Integer       value);
      void Set(const String& name, const Binary& value);

      // create new or set existing entry
      void SetOrCreate(const String& name, const String& value);
      void SetOrCreate(const String& name, Integer       value);
      void SetOrCreate(const String& name, const Binary& value);

      // get value, entry has to exist
      String GetString(const String& name) const;
      Integer GetInteger(const String& name) const;
      Binary GetBinary(const String& name) const;

      // returns true if name was deleted, false if name was not found or name has children and recursive == false
      bool TryDelete(const String& name, bool recursive = true);
      // throws EntryNotFound if name does not exist
      // throws HasChildEntry if recursive == false and name has children
      void Delete(const String& name, bool recursive = true);

      // slow, depends on number of entries in DB! >= O(n)!
      void CheckDataConsistency() const;

      // slow, depends on number of entries in DB! >= O(n)!
      // returns number of moved entries
      Integer RepairDataConsistency();

    private:
      enum class SettingType { Integer, String, Binary };

      // default entry type
      using DefaultEntryType = Integer;

      using Path = std::vector<Store::String>;

      using CachedStatement = std::shared_ptr<SQLite::Statement>;
      using StatementCache = std::map<std::string, CachedStatement>;

      using RandomNumberGenerator = std::unique_ptr<Detail::RandomNumberGenerator>;

      using Database = std::unique_ptr<SQLite::Database>;
      using Statement = std::unique_ptr<SQLite::Statement>;

      friend class ReadOnlyTransaction;
      friend class WriteableTransaction;
      
      // this is a somewhat dirty trick to get access to private members in Store objects ...
      friend struct Configuration::UnitTest::Detail::PrivateAccess;

      // returns true if delimiter is not found in any name currentl present in the store
      // returning true implies that SetNewDelimiter() with the given delimiter will be successful
      bool IsValidNewDelimiter(String::value_type delimiter) const;
      // throws exception if delimiter can not be set
      void SetNewDelimiter(String::value_type delimiter);


      std::shared_ptr<SQLite::Transaction> GetTransaction(bool exclusive) const;

      bool SettingExists(const std::string& name) const;
      
      SettingType GetSettingType(const std::string& name) const;

      // TODO: rework to also use a value binder!
      template <typename Value>
      void SetSettingImpl(const std::string& name, const Value& value);
      void SetSettingImpl(const std::string& name, const Store::Binary& value);

      void SetSetting(const std::string& name, Integer value);
      void SetSetting(const std::string& name, const String& value);
      void SetSetting(const std::string& name, const Binary& value);

      Statement GetSetting(const std::string& name, int type = -1) const;

      Integer GetSettingInt(const std::string& name) const;
      String GetSettingStr(const std::string& name) const;
      Binary GetSettingBin(const std::string& name) const;

      void GetAndCheckConfiguration(wchar_t nameDelimiter);
      void CheckOrSetRootEntry();


      using IdList = std::vector<Integer>;
      static_assert((sizeof(IdList::value_type) * 8) >= 64, "Entry ids must be at least 64 bits wide");
      static_assert(std::is_same<IdList::value_type, Store::Integer>::value, "We currently require Entry ids to be Store::Integer, implementation detail");


      Path Store::ParseName(const Store::String& name) const;

      bool Exists(const Path& path) const;

      ValueType GetEntryType(const Path& path) const;
      ValueType GetEntryType(Integer id) const;

      // on failure <idPath> will contain all valid parent ids in the path or is empty if there is none (excl. root/Id(0) !)
      // on failure <lastValid> will point to the last valid name in the path or will be unchanged if there is none
      bool GetEntryId(IdList& idPath, Path::const_iterator& lastValid, const Path& path, Integer parent = 0) const;
      bool GetEntryId(IdList& idPath, const Path& path, Integer parent = 0) const;
      IdList GetEntryId(const Path& path, Integer parent = 0) const;
      bool GetEntryId(IdList& idPath, const String& name, Integer parent = 0) const;
      IdList GetEntryId(const String& entryName, Integer parent = 0) const;


      using ValueBinder = std::function<void(int, SQLite::Statement&)>;
      using ValueGetter = std::function<void(SQLite::Statement&)>;

      bool Store::HasChild(Integer parent) const;

      Integer GetEntryRevision(Integer id) const;

      Integer GetRandomRevision();
      // bumps revision of the root entry and all ids in idPath, idPath may be empty
      void UpdateRevision(IdList::const_iterator first, IdList::const_iterator last);

      void SetEntry(const IdList& idPath, ValueType type, const ValueBinder& bindValue);
      void SetEntry(const String& name, ValueType type, const ValueBinder& bindValue);

      void CreateEntry(Integer parent, const String& name, ValueType type, const ValueBinder& bindValue);
      void CreateEntry(IdList parentPath, Path::const_iterator first, const Path::const_iterator& last, ValueType type, const ValueBinder& bindValue);
      void CreateEntry(const Path& path, ValueType type, const ValueBinder& bindValue);

      void SetOrCreate(const Path& path, ValueType type, const ValueBinder& bindValue);

      void GetEntryValue(const Path& path, ValueType type, const ValueGetter& getValue) const;

      IdList GetChildEntries(Integer parent) const;
      Children GetChildEntryNames(Integer parent) const;

      // do not use directly, always call TryDeleteEntry() !
      bool TryDeleteEntryImpl(Integer id, bool recursive);
      bool TryDeleteEntry(const IdList& idPath, bool recursive);
                  
      String PathToName(const Path& path) const;
      String ValueTypeToString(ValueType type) const;

      void TraverseChildren(Integer id, std::function<void(Integer)> func) const;

      CachedStatement GetStatement(const std::string& statementText) const;

      static const Integer CurrentMajorVersion;
      static const Integer CurrentMinorVersion;

      // default entry value
      static const ValueType        DefaultEntryValueType;
      static const DefaultEntryType DefaultEntryValue;

      // variables
      mutable Database m_Database;

      Integer m_DatabaseVersionMajor;
      Integer m_DatabaseVersionMinor;

      String::value_type m_Delimiter;

      mutable std::weak_ptr<SQLite::Transaction> m_Transaction;
      mutable bool                               m_WriteableTransaction;

      mutable StatementCache m_StatementCache;

      RandomNumberGenerator m_RandomNumberGenerator;
  };

  // transactions are non-copyable (incl. move assignment!) but support move construction 
  class ReadOnlyTransaction : private boost::noncopyable
  {
    public:
      explicit ReadOnlyTransaction(const Store& store);

      ~ReadOnlyTransaction() noexcept;

    private:
      std::shared_ptr<SQLite::Transaction> m_Transaction;      
  };

  class WriteableTransaction : private boost::noncopyable
  {
  public:
    explicit WriteableTransaction(Store& store);

    ~WriteableTransaction() noexcept;

    void Commit();

  private:
    bool                                 m_Commited;
    std::string                          m_SavepointName;
    std::shared_ptr<SQLite::Transaction> m_Transaction;
  };


  class ExceptionInterface
  {
    public:
      virtual ~ExceptionInterface() noexcept {}

      virtual const std::wstring& What() const noexcept = 0;
      virtual const std::wstring& TypeName() const noexcept = 0;
  };

  struct Exception : virtual std::exception, ExceptionInterface {};

  struct RuntimeError : Exception {};
  
  struct NotFound :          RuntimeError {};
  struct SettingNotFound :   NotFound {};
  struct EntryNotFound :     NotFound {};
  struct InvalidName :       RuntimeError {};
  struct NameAlreadyExists : RuntimeError {};
  struct HasChildEntry :     RuntimeError {};
  struct WrongValueType :    RuntimeError {};

  struct DatabaseError :      RuntimeError {};
  struct InvalidQuery :       DatabaseError {};
  struct InvalidInsert :      DatabaseError {};
  struct InvalidTransaction : DatabaseError {};
  struct InvalidDelimiter :   DatabaseError {};

  struct InconsistenData :       DatabaseError {};
  struct RootEntryMissing :      InconsistenData {};
  struct MultipleRootEntries :   InconsistenData {};
  struct InvalidRootEntry :      InconsistenData {};
  struct InvalidEntryNameFound : InconsistenData {};
  struct EntryIdNotUnique :      InconsistenData {};
  struct AbandonedEntry :        InconsistenData {};
  struct InvalidEntryLinking :   InconsistenData {};
  struct UnknownEntryType :      InconsistenData {};

  struct ConfigurationError :      DatabaseError {};
  struct UnknownDataType :         ConfigurationError {};
  struct DataTypeMissmatch :       ConfigurationError {};
  struct VersionNotSupported :     ConfigurationError {};
  struct InvalidConfiguration :    ConfigurationError {};
  struct InvalidDelimiterSetting : InvalidConfiguration {};

}


#endif
