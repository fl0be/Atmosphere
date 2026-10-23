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

    enum AllocationType {
        AllocationType_FileName,
        AllocationType_DirName,
        AllocationType_FullPath,
        AllocationType_SourceInfo,
        AllocationType_BuildFileContext,
        AllocationType_BuildDirContext,
        AllocationType_TableCache,
        AllocationType_DirPointerArray,
        AllocationType_DirContextSet,
        AllocationType_FileContextSet,
        AllocationType_Memory,

        AllocationType_Count,
    };

    class ScopedHeapLock {
        NON_COPYABLE(ScopedHeapLock);
        NON_MOVEABLE(ScopedHeapLock);
        public:
            explicit ScopedHeapLock(ncm::ProgramId program_id);
            ~ScopedHeapLock();
    };

    void *AllocateTrackedImpl(AllocationType type, size_t size);

    template<typename T = void>
    ALWAYS_INLINE T *AllocateTracked(AllocationType type, size_t size, bool abort_on_failure = true) {
        void * const mem = AllocateTrackedImpl(type, size);
        AMS_ABORT_UNLESS(!abort_on_failure || mem != nullptr);
        return static_cast<T *>(mem);
    }

    void FreeTracked(AllocationType type, void *p, size_t size);

    template<typename T, typename... Args>
    T *AllocateTyped(AllocationType type, Args &&... args) {
        return std::construct_at(AllocateTracked<T>(type, sizeof(T)), std::forward<Args>(args)...);
    }

    template<AllocationType AllocType, typename T>
    class TrackedAllocator {
        public:
            using value_type = T;

            template<typename U>
            struct rebind {
                using other = TrackedAllocator<AllocType, U>;
            };
        public:
            TrackedAllocator() = default;

            T *allocate(size_t n) {
                return AllocateTracked<T>(AllocType, sizeof(T) * n);
            }

            void deallocate(T *p, size_t n) {
                FreeTracked(AllocType, p, sizeof(T) * n);
            }
    };

    Result ConfigureDynamicHeap(u64 *out_size, ncm::ProgramId program_id, const cfg::OverrideStatus &status, bool is_application);

}
