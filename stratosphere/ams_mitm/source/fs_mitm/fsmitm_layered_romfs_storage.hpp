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
#include "fsmitm_romfs_image.hpp"

namespace ams::mitm::fs {

    class LayeredRomfsStorageImpl {
        private:
            romfs::Image m_romfs;
            os::Event m_initialize_event;
            bool m_is_initialized;
            bool m_started_initialize;
        public:
            LayeredRomfsStorageImpl(std::unique_ptr<ams::fs::IStorage> s_r, std::unique_ptr<ams::fs::IStorage> f_r, ncm::ProgramId pr_id);

            void BeginInitialize();
            void InitializeImpl();

            constexpr ncm::ProgramId GetProgramId() const { return m_romfs.GetProgramId(); }

            Result Read(s64 offset, void *buffer, size_t size);
            Result Flush();
            Result GetSize(s64 *out_size);
            Result OperateRange(void *dst, size_t dst_size, ams::fs::OperationId op_id, s64 offset, s64 size, const void *src, size_t src_size);
    };

    std::shared_ptr<ams::fs::IStorage> GetLayeredRomfsStorage(ncm::ProgramId program_id, ::FsStorage &data_storage, bool is_process_romfs);
    std::shared_ptr<ams::fs::IStorageForBatchRead> GetLayeredRomfsStorageForBatchRead(ncm::ProgramId program_id, ::FsStorageForBatchRead &data_storage, bool is_process_romfs);
    void FinalizeLayeredRomfsStorage(ncm::ProgramId program_id);

}
