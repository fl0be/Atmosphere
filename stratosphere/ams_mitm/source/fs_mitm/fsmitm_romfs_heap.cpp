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
#include <stratosphere.hpp>
#include "../amsmitm_fs_utils.hpp"
#include "fsmitm_romfs_heap.hpp"
#include "fsmitm_layered_romfs_storage.hpp"
#include "memlet/memlet.h"

namespace ams::mitm::fs {

    using namespace ams::fs;

    namespace romfs {

        namespace {

            constexpr size_t MaximumRomfsBuildAppletMemorySize = 32_MB;

            struct ApplicationWithDynamicHeapInfo {
                ncm::ProgramId program_id;
                size_t dynamic_app_heap_size;
                size_t dynamic_system_heap_size;
            };

            constexpr const ApplicationWithDynamicHeapInfo ApplicationsWithDynamicHeap[] = {
                /* Danganronpa V3: Killing Harmony Anniversary Edition. */
                /* Requirement ? MB. 16 MB stolen heap fixes a crash, though. */
                /* Unknown heap sensitivity. */
                { 0x010063F014176000,  16_MB, 0_MB },

                /* Until Then. */
                /* Requirement ~34 MB. */
                /* No particular heap sensitivity. */
                { 0x010019C023004000,  16_MB, 0_MB },

                /* Trails in the Sky 1st Chapter. */
                /* Requirement ? MB. 16 MB stolen heap fixes a crash, though. */
                /* Unknown heap sensitivity. */
                { 0x01002C9022770000,  16_MB, 0_MB },

                /* Trails in the Sky 2nd Chapter (Global, Asia and Japan versions). */
                /* Requirement ~16 MB.*/
                /* No particular heap sensitivity. */
                { 0x01007A2027548000,  8_MB, 0_MB },
                { 0x0100E8B028B54000,  8_MB, 0_MB },
                { 0x01002270268E0000,  8_MB, 0_MB },

                /* STAR WARS: Knights of the Old Republic II: The Sith Lords. */
                /* Requirement ? MB. 16 MB stolen heap fixes a crash, though. */
                /* Unknown heap sensitivity. */
                { 0x0100B2C016252000,  16_MB, 0_MB },

                /* Animal Crossing: New Horizons. */
                /* Requirement ~24 MB. */
                /* No particular heap sensitivity. */
                { 0x01006F8002326000,  16_MB, 0_MB },

                /* Fire Emblem: Engage. */
                /* Requirement ~32+ MB. */
                /* No particular heap sensitivity. */
                { 0x0100A6301214E000,  20_MB, 0_MB },

                /* The Legend of Zelda: Tears of the Kingdom. */
                /* Requirement ~48 MB. */
                /* Game is highly sensitive to memory stolen from application heap. */
                /* 1.0.0 tolerates no more than 16 MB stolen. 1.1.0 no more than 12 MB. */
                { 0x0100F2C0115B6000,  10_MB, 8_MB },
            };

            constexpr size_t GetDynamicAppHeapSize(ncm::ProgramId program_id) {
                for (const auto &info : ApplicationsWithDynamicHeap) {
                    if (info.program_id == program_id) {
                        return info.dynamic_app_heap_size;
                    }
                }

                return 0;
            }

            constexpr size_t GetDynamicSysHeapSize(ncm::ProgramId program_id) {
                for (const auto &info : ApplicationsWithDynamicHeap) {
                    if (info.program_id == program_id) {
                        return info.dynamic_system_heap_size;
                    }
                }

                return 0;
            }

            template<auto MapImpl, auto UnmapImpl>
            struct DynamicHeap {
                uintptr_t heap_address{};
                size_t heap_size{};
                size_t outstanding_allocations{};
                util::TypedStorage<mem::StandardAllocator> heap{};
                os::SdkMutex release_heap_lock{};

                constexpr DynamicHeap() = default;

                void Map() {
                    if (this->heap_address == 0) {
                        /* NOTE: Lock not necessary, because this is the only location which do 0 -> non-zero. */

                        R_ABORT_UNLESS(MapImpl(std::addressof(this->heap_address), this->heap_size));
                        AMS_ABORT_UNLESS(this->heap_address != 0 || this->heap_size == 0);

                        /* Create heap. */
                        if (this->heap_size > 0) {
                            util::ConstructAt(this->heap, reinterpret_cast<void *>(this->heap_address), this->heap_size);
                        }
                    }
                }

                void TryRelease() {
                    if (this->outstanding_allocations == 0) {
                        std::scoped_lock lk(this->release_heap_lock);

                        if (this->heap_address != 0) {
                            util::DestroyAt(this->heap);
                            this->heap = {};

                            R_ABORT_UNLESS(UnmapImpl(this->heap_address, this->heap_size));

                            this->heap_address = 0;
                        }
                    }
                }

                void *Allocate(size_t size) {
                    void * const ret = util::GetReference(this->heap).Allocate(size);
                    if (AMS_LIKELY(ret != nullptr)) {
                        ++this->outstanding_allocations;
                    }
                    return ret;
                }

                bool TryFree(void *p) {
                    if (this->IsAllocated(p)) {
                        --this->outstanding_allocations;

                        util::GetReference(this->heap).Free(p);

                        return true;
                    } else {
                        return false;
                    }
                }

                bool IsAllocated(void *p) const {
                    const uintptr_t address = reinterpret_cast<uintptr_t>(p);

                    return this->heap_address != 0 && (this->heap_address <= address && address < this->heap_address + this->heap_size);
                }

                void Reset() {
                    /* This should require no remaining allocations. */
                    AMS_ABORT_UNLESS(this->outstanding_allocations == 0);

                    /* Free the heap. */
                    this->TryRelease();
                    AMS_ABORT_UNLESS(this->heap_address == 0);

                    /* Clear the heap size. */
                    this->heap_size = 0;
                }
            };

            Result MapByHeap(uintptr_t *out, size_t size) {
                R_TRY(os::SetMemoryHeapSize(size));
                R_RETURN(os::AllocateMemoryBlock(out, size));
            }

            Result UnmapByHeap(uintptr_t address, size_t size) {
                os::FreeMemoryBlock(address, size);
                R_RETURN(os::SetMemoryHeapSize(0));
            }

            constinit os::SharedMemoryType g_applet_shared_memory;

            Result MapAppletMemory(uintptr_t *out, size_t &size) {
                /* Ensure that we can try to get a native handle for the shared memory. */
                AMS_FUNCTION_LOCAL_STATIC_CONSTINIT(bool, s_initialized_memlet, false);
                if (AMS_UNLIKELY(!s_initialized_memlet)) {
                    R_ABORT_UNLESS(::memletInitialize());
                    s_initialized_memlet = true;
                }

                /* Try to get a shared handle for the memory. */
                ::Handle shmem_handle = INVALID_HANDLE;
                u64 shmem_size = 0;
                if (R_FAILED(::memletCreateAppletSharedMemory(std::addressof(shmem_handle), std::addressof(shmem_size), size))) {
                    /* If we fail, set the heap size to 0. */
                    size = 0;
                    R_SUCCEED();
                }

                /* Set the output size. */
                size = shmem_size;

                /* Setup the shared memory. */
                os::AttachSharedMemory(std::addressof(g_applet_shared_memory), shmem_size, shmem_handle, true);

                /* Map the shared memory. */
                void *mem = os::MapSharedMemory(std::addressof(g_applet_shared_memory), os::MemoryPermission_ReadWrite);
                AMS_ABORT_UNLESS(mem != nullptr);

                /* Set the output. */
                *out = reinterpret_cast<uintptr_t>(mem);
                R_SUCCEED();
            }

            Result UnmapAppletMemory(uintptr_t, size_t) {
                /* Check that it's possible for us to unmap. */
                AMS_ABORT_UNLESS(os::GetSharedMemoryHandle(std::addressof(g_applet_shared_memory)) != os::InvalidNativeHandle);

                /* Unmap. */
                os::DestroySharedMemory(std::addressof(g_applet_shared_memory));

                /* Check that we unmapped successfully. */
                AMS_ABORT_UNLESS(os::GetSharedMemoryHandle(std::addressof(g_applet_shared_memory)) == os::InvalidNativeHandle);
                R_SUCCEED();
            }

            /* Dynamic allocation globals. */
            constinit ncm::ProgramId g_dynamic_heap_program_id{};

            constinit bool g_building_from_dynamic_heap = false;

            constinit DynamicHeap<os::AllocateUnsafeMemory, os::FreeUnsafeMemory> g_dynamic_app_heap;
            constinit DynamicHeap<MapByHeap, UnmapByHeap> g_dynamic_sys_heap;
            constinit DynamicHeap<MapAppletMemory, UnmapAppletMemory> g_dynamic_let_heap;

            constexpr bool CanAllocateFromDynamicAppletHeap(AllocationType type) {
                switch (type) {
                    case AllocationType_FullPath:
                    case AllocationType_SourceInfo:
                    case AllocationType_Memory:
                    case AllocationType_TableCache:
                        return false;
                    default:
                        return true;
                }
            }

        }

        void *AllocateTracked(AllocationType type, size_t size) {
            if (g_building_from_dynamic_heap) {
                void *ret = nullptr;
                if (CanAllocateFromDynamicAppletHeap(type) && g_dynamic_let_heap.heap_address != 0) {
                    ret = g_dynamic_let_heap.Allocate(size);
                }

                if (ret == nullptr && g_dynamic_app_heap.heap_address != 0) {
                    ret = g_dynamic_app_heap.Allocate(size);
                }

                if (ret == nullptr && g_dynamic_sys_heap.heap_address != 0) {
                    ret = g_dynamic_sys_heap.Allocate(size);
                }

                if (ret == nullptr) {
                    ret = std::malloc(size);
                }

                return ret;
            } else {
                return std::malloc(size);
            }
        }

        void FreeTracked(AllocationType type, void *p, size_t size) {
            AMS_UNUSED(type);
            AMS_UNUSED(size);

            if (g_dynamic_let_heap.TryFree(p)) {
                if (!g_building_from_dynamic_heap) {
                    g_dynamic_let_heap.TryRelease();
                }
            } else if (g_dynamic_app_heap.TryFree(p)) {
                if (!g_building_from_dynamic_heap) {
                    g_dynamic_app_heap.TryRelease();
                }
            } else if (g_dynamic_sys_heap.TryFree(p)) {
                if (!g_building_from_dynamic_heap) {
                    g_dynamic_sys_heap.TryRelease();
                }
            } else {
                std::free(p);
            }
        }

        void InitializeDynamicHeapForBuildRomfs(ncm::ProgramId program_id) {
            if (program_id == g_dynamic_heap_program_id && g_dynamic_app_heap.heap_size > 0) {
                /* This romfs will build out of dynamic heap. */
                g_building_from_dynamic_heap = true;

                g_dynamic_app_heap.Map();

                if (g_dynamic_sys_heap.heap_size > 0) {
                    g_dynamic_sys_heap.Map();
                }

                if (g_dynamic_let_heap.heap_size > 0) {
                    g_dynamic_let_heap.Map();
                }
            }
        }

        void FinalizeDynamicHeapForBuildRomfs() {
            /* We are definitely no longer building out of dynamic heap. */
            g_building_from_dynamic_heap = false;

            g_dynamic_app_heap.TryRelease();
            g_dynamic_let_heap.Reset();
        }

        Result ConfigureDynamicHeap(u64 *out_size, ncm::ProgramId program_id, const cfg::OverrideStatus &status, bool is_application) {
            /* Baseline: use no dynamic heap. */
            *out_size = 0;

            /* If the process is not an application, we do not care about dynamic heap. */
            R_SUCCEED_IF(!is_application);

            /* First, we need to ensure that, if the game used dynamic heap, we clear it. */
            if (g_dynamic_app_heap.heap_size > 0) {
                mitm::fs::FinalizeLayeredRomfsStorage(g_dynamic_heap_program_id);

                /* Free the heap. */
                g_dynamic_app_heap.Reset();
                g_dynamic_sys_heap.Reset();
            }

            /* Next, if we aren't going to end up building a romfs, we can ignore dynamic heap. */
            R_SUCCEED_IF(!status.IsProgramSpecific());

            /* Only mitm if there is actually an override romfs. */
            R_SUCCEED_IF(!mitm::fs::HasSdRomfsContent(program_id));

            /* Next, set the new program id for dynamic heap. */
            g_dynamic_heap_program_id    = program_id;
            g_dynamic_app_heap.heap_size = GetDynamicAppHeapSize(g_dynamic_heap_program_id);
            g_dynamic_sys_heap.heap_size = GetDynamicSysHeapSize(g_dynamic_heap_program_id);
            g_dynamic_let_heap.heap_size = MaximumRomfsBuildAppletMemorySize;

            /* Set output. */
            *out_size = g_dynamic_app_heap.heap_size;

            R_SUCCEED();
        }

    }

}
