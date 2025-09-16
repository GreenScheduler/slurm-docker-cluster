-- /etc/slurm/job_submit.lua
-- Keep all lowcarbon jobs pending by default, by setting a far-future BeginTime.

local LOWCARBON_PART = "lowcarbon"
local SECONDS_IN_YEAR = 365 * 24 * 60 * 60

local function gate_if_lowcarbon(job_desc)
    -- job_desc.partition may be nil if user didn’t specify; we only gate when
    -- the partition *is* explicitly lowcarbon (adjust this policy if you route jobs).
    if job_desc.partition == LOWCARBON_PART then
        -- Only set a gate if user didn’t already choose a BeginTime
        if job_desc.begin_time == 0 or job_desc.begin_time == slurm.NO_VAL then
            job_desc.begin_time = os.time() + SECONDS_IN_YEAR
            slurm.log_info("gate_if_lowcarbon: setting begin time for job to %d", job_desc.begin_time)
            local tag = "lowcarbon:held"
            if job_desc.comment == nil then
                job_desc.comment = tag
            else
                job_desc.comment = tostring(job_desc.comment) .. "," .. tag
            end
        end
    end
    return slurm.SUCCESS
end

function slurm_job_submit(job_desc, part_list, submit_uid)
    return gate_if_lowcarbon(job_desc)
end

function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
    -- Re-apply gate on modification if user keeps the job in lowcarbon
    return gate_if_lowcarbon(job_desc)
end

return slurm.SUCCESS
