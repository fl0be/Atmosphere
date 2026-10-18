/*
 * Copyright (c) Atmosphère-NX
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stratosphere.hpp>

namespace ams::mitm::fs::romfs {

    constexpr u32 EmptyEntry = 0xFFFFFFFF;
    constexpr size_t FilePartitionOffset = 0x200;

    struct Header {
        s64 header_size;
        s64 dir_hash_table_ofs;
        s64 dir_hash_table_size;
        s64 dir_table_ofs;
        s64 dir_table_size;
        s64 file_hash_table_ofs;
        s64 file_hash_table_size;
        s64 file_table_ofs;
        s64 file_table_size;
        s64 file_partition_ofs;
    };
    static_assert(util::is_pod<Header>::value && sizeof(Header) == 0x50);

    struct DirectoryEntry {
        u32 parent;
        u32 sibling;
        u32 child;
        u32 file;
        u32 hash;
        u32 name_size;
        char name[];
    };
    static_assert(util::is_pod<DirectoryEntry>::value && sizeof(DirectoryEntry) == 0x18);

    struct FileEntry {
        u32 parent;
        u32 sibling;
        s64 offset;
        s64 size;
        u32 hash;
        u32 name_size;
        char name[];
    };
    static_assert(util::is_pod<FileEntry>::value && sizeof(FileEntry) == 0x20);

    constexpr inline u32 CalculatePathHash(u32 parent, const char *path, u32 start, size_t path_len) {
        u32 hash = parent ^ 123456789;
        for (size_t i = 0; i < path_len; i++) {
            hash = (hash >> 5) | (hash << 27);
            hash ^= static_cast<unsigned char>(path[start + i]);
        }
        return hash;
    }

    constexpr inline size_t GetHashTableSize(size_t num_entries) {
        if (num_entries < 3) {
            return 3;
        } else if (num_entries < 19) {
            return num_entries | 1;
        } else {
            size_t count = num_entries;
            while ((count %  2 == 0) ||
                   (count %  3 == 0) ||
                   (count %  5 == 0) ||
                   (count %  7 == 0) ||
                   (count % 11 == 0) ||
                   (count % 13 == 0) ||
                   (count % 17 == 0))
           {
               count++;
           }
           return count;
        }
    }

}
