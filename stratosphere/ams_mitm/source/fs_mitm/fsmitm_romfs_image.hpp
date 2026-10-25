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
#include "fsmitm_romfs_sources.hpp"
#include "fsmitm_romfs_metadata.hpp"

namespace ams::mitm::fs::romfs {

    class Image {
        NON_COPYABLE(Image);
        NON_MOVEABLE(Image);
        private:
            std::unique_ptr<ams::fs::IStorage> m_storage_romfs;
            std::unique_ptr<ams::fs::IStorage> m_file_romfs;
            ncm::ProgramId m_program_id;
            RomfsHeader m_header;
            MetadataFile m_metadata;
            SourceInfoVector m_source_infos;
        public:
            Image(std::unique_ptr<ams::fs::IStorage> s_r, std::unique_ptr<ams::fs::IStorage> f_r, ncm::ProgramId pr_id);
            ~Image();

            void Initialize(bool walk_sd);

            constexpr ncm::ProgramId GetProgramId() const { return m_program_id; }

            s64 GetSize() const {
                const auto &back = m_source_infos.back();
                return back.virtual_offset + back.size;
            }

            Result Read(s64 offset, void *buffer, size_t size) const;
    };

}
