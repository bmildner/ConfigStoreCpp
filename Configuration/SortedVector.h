// Copyright (c) 2014 by Bertolt Mildner
// All rights reserved.

#ifndef CONFIGURATION_SORTEDVECTOR_H
#define CONFIGURATION_SORTEDVECTOR_H

#pragma once

#include <vector>
#include <algorithm>
#include <cassert>


namespace Configuration
{
  namespace Detail
  {

    // WARNING: incomplete and highly experimental code! DO NOT USE!
    
    template <typename T, typename Allocator = std::vector<T>::allocator_type>
    class sorted_vector
    {
      private:
        using Vector = std::vector<T, Allocator>;

      public:
        using value_type             = typename Vector::value_type;
        using allocator_type         = typename Vector::allocator_type;
        using size_type              = typename Vector::size_type;
        using difference_type        = typename Vector::difference_type;
        using reference              = typename Vector::reference;
        using const_reference        = typename Vector::const_reference;
        using pointer                = typename Vector::pointer;
        using const_pointer          = typename Vector::const_pointer;
        using iterator               = typename Vector::iterator;
        using const_iterator         = typename Vector::const_iterator;
        using reverse_iterator       = typename Vector::reverse_iterator;
        using const_reverse_iterator = typename Vector::const_reverse_iterator;

        // c'tors

        sorted_vector() {};

        explicit sorted_vector(const Allocator& alloc) : m_Vector(alloc) {}

        explicit sorted_vector(const sorted_vector& other) = default;

        sorted_vector(const sorted_vector& other, const Allocator& alloc)
        : m_Vector(other.m_Vector, alloc)
        {
          assert(is_sorted());
        }

        template <typename InputIt>
        inline sorted_vector(InputIt first, InputIt last, const Allocator& alloc = Allocator())
        : m_Vector(first, last, alloc)
        {
          sort();
        }

        sorted_vector(std::initializer_list<T> init, const Allocator& alloc = Allocator())
        : m_Vector(init, alloc)
        {
          sort();
        }

        sorted_vector(sorted_vector&& other)
        : m_Vector(std::move(other.m_Vector))
        {
          assert(is_sorted());
        }

        sorted_vector(sorted_vector&& other, const Allocator& alloc)
        : m_Vector(std::move(other.m_Vector), alloc)
        {
          assert(is_sorted());
        }

        // assignment operators

        sorted_vector& operator=(const sorted_vector& other)
        {
          Vector tempVec = other.m_Vector;

          m_Vector.swap(tempVec);

          assert(is_sorted());

          return *this;
        }
        
        sorted_vector& operator=(sorted_vector&& other)
        {
          this->m_Vector.swap(other.m_Vector);

          assert(is_sorted());

          return *this;
        }

        sorted_vector& operator=(std::initializer_list<T> ilist)
        {
          Vector tempVec = ilist;
          
          sort();

          m_Vector.swap(tempVec);

          assert(is_sorted());

          return *this;
        }

        // get allocator

        allocator_type get_allocator() const
        {
          return m_Vector.get_allocator();
        }

        // element access 

        reference at(size_type pos)
        {
          return m_Vector.at(pos);
        }

        const_reference at(size_type pos) const
        {
          return m_Vector.at(pos);
        }

        reference operator[](size_type pos)
        {
          return m_Vector[pos];
        }

        /*constexpr*/ const_reference operator[](size_type pos) const  // TODO: constexpr
        {
          return m_Vector[pos];
        }

        reference front()
        {
          return m_Vector.front();
        }

        const_reference front() const
        {
          return m_Vector.front();
        }

        reference back()
        {
          return m_Vector.back();
        }

        const_reference back() const
        {
          return m_Vector.back();
        }

        T* data() noexcept
        {
          return m_Vector.data();
        }
        
        const T* data() const noexcept
        {
          return m_Vector.data();
        }

        // iterators

        iterator begin() noexcept
        {
          return m_Vector.begin();
        }

        const_iterator begin() const noexcept
        {
          return m_Vector.begin();
        }

        const_iterator cbegin() const noexcept
        {
          return m_Vector.cbegin();
        }

        iterator end() noexcept
        {
          return m_Vector.end();
        }

        const_iterator end() const noexcept
        {
          return m_Vector.end();
        }

        const_iterator cend() const noexcept
        {
          return m_Vector.cend();
        }

        reverse_iterator rbegin() noexcept
        {
          return m_Vector.rbegin();
        }

        const_reverse_iterator rbegin() const noexcept
        {
          return m_Vector.rbegin();
        }

        const_reverse_iterator crbegin() const noexcept
        {
          return m_Vector.crbegin();
        }

        reverse_iterator rend() noexcept
        {
          return m_Vector.rend();
        }

        const_reverse_iterator rend() const noexcept
        {
          return m_Vector.rend();
        }

        const_reverse_iterator crend() const noexcept
        {
          return m_Vector.crend();
        }

        // capacity 

        bool empty() const noexcept
        {
          return m_Vector.empty();
        }

        size_type size() const noexcept
        {
          return m_Vector.size();
        }

        size_type max_size() const noexcept
        {
          return m_Vector.max_size();
        }

        void reserve(size_type new_cap)
        {
          m_Vector.reserve(new_cap);
        }

        size_type capacity() const noexcept
        {
          return m_Vector.capacity();
        }

        void shrink_to_fit()
        {
          m_Vector.shrink_to_fit();
        }

        // modifiers 

        void clear() noexcept
        {
          m_Vector.clear();
        }

        iterator insert(const_iterator pos, const T& value)
        {
          // TODO: sort
          return m_Vector.insert(pos, value);
        }

        iterator insert(const_iterator pos, T&& value)
        {
          // TODO: sort
          return m_Vector.insert(pos, std::move(value));
        }

        iterator insert(const_iterator pos, size_type count, const T& value)
        {
          // TODO: sort
          return m_Vector.insert(pos, count, value);
        }

        template <class InputIt>
        inline iterator insert(const_iterator pos, InputIt first, InputIt last)
        {
          // TODO: sort
          return m_Vector.insert(pos, first, last);
        }

        iterator insert(const_iterator pos, std::initializer_list<T> ilist)
        {
          // TODO: sort
          return m_Vector.insert(pos, ilist);
        }

        template <typename... Args>
        iterator emplace(const_iterator pos, Args&&... args)
        {
          m_Vector.emplace(pos, std::forward<Args>(args)...);
          // TODO: sort
        }

        iterator erase(const_iterator pos)
        {
          return m_Vector.erase(pos);
        }

        iterator erase(const_iterator first, const_iterator last)
        {
          return m_Vector.erase(first, last);
        }

        void push_back(const T& value)
        {
          m_Vector.push_back(value);
          // TODO: sort
        }

        void push_back(T&& value)
        {
          m_Vector.push_back(std::forward<T>(value));
          // TODO: sort
        }

        template <typename... Args>
        void emplace_back(Args&&... args)
        {
          m_Vector.emplace_back(std::forward<Args>(args)...);
          // TODO: sort
        }

        void pop_back()
        {
          m_Vector.pop_back();
        }

        void resize(size_type count)
        {
          m_Vector.resize(count);
          // TODO: sort
        }

        void resize(size_type count, const value_type& value)
        {
          m_Vector.resize(count, value);
          // TODO: sort
        }

        void swap(sorted_vector& other)
        {
          m_Vector.swap(other.m_Vector);
        }

      private:
        Vector m_Vector;

        void sort()
        {
          using std::sort;

          sort(m_Vector.begin(), m_Vector.end());
        }

        bool is_sorted() const
        {
          return std::is_sorted(m_Vector.begin(), m_Vector.end());
        }
    };


    template <typename T, typename Alloc>
    bool operator==(const sorted_vector<T, Alloc>& lhs, const sorted_vector<T, Alloc>& rhs)
    {
      return lhs.m_Vector == rhs.m_Vector;
    }

    template <typename T, typename Alloc>
    bool operator!=(const sorted_vector<T, Alloc>& lhs, const sorted_vector<T, Alloc>& rhs)
    {
      return lhs.m_Vector != rhs.m_Vector;
    }

    template <typename T, typename Alloc>
    bool operator<(const sorted_vector<T, Alloc>& lhs, const sorted_vector<T, Alloc>& rhs)
    {
      return lhs.m_Vector < rhs.m_Vector;
    }

    template <typename T, typename Alloc>
    bool operator<=(const sorted_vector<T, Alloc>& lhs, const sorted_vector<T, Alloc>& rhs)
    {
      return lhs.m_Vector <= rhs.m_Vector;
    }

    template <typename T, typename Alloc>
    bool operator>(const sorted_vector<T, Alloc>& lhs, const sorted_vector<T, Alloc>& rhs)
    {
      return lhs.m_Vector > rhs.m_Vector;
    }

    template <typename T, typename Alloc>
    bool operator>=(const sorted_vector<T, Alloc>& lhs, const sorted_vector<T, Alloc>& rhs)
    {
      return lhs.m_Vector >= rhs.m_Vector;
    }

    template <typename T, typename Alloc>
    void swap(sorted_vector<T, Alloc>& lhs, sorted_vector<T, Alloc>& rhs)
    {
      lhs.swap(rhs);
    }

  }  // namespace Detail
}  // namespace Configuration

#endif
