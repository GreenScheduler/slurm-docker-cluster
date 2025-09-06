-- /etc/slurm/job_submit.lua
-- Keep all lowcarbon jobs pending by default, by setting a far-future BeginTime.
-- Your daemon will later "pull" BeginTime back into a green window via scontrol.

local LOWCARBON_PART = "lowcarbon"
local SECONDS_IN_YEAR = 365 * 24 * 60 * 60

local function gate_if_lowcarbon(job_desc)
    -- job_desc.partition may be nil if user didn’t specify; we only gate when
    -- the partition *is* explicitly lowcarbon (adjust this policy if you route jobs).
    if job_desc.partition == LOWCARBON_PART then
        -- Only set a gate if user didn’t already choose a BeginTime
        if job_desc.begin_time == nil or job_desc.begin_time == slurm.NO_VAL then
            -- Far-future gate (now + 1 year). Daemon will bring this back.
            job_desc.begin_time = os.time() + SECONDS_IN_YEAR
            -- Optional breadcrumb so your daemon knows this was gated by policy
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

