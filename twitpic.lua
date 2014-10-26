local url_count = 0
local tries = 0
local item_type = os.getenv('item_type')
local item_value = os.getenv('item_value')
local escaped_item_name = os.getenv('escaped_item_name')
local item_dir = os.getenv('item_dir')


read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end


wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}

  if item_type == "image" then
    if string.match(url, "twitpic%.com/") then
      html = read_file(file)

      for video_url in string.gmatch(html, '<source src="(http[^"]+)') do
        table.insert(urls, { url=video_url })
      end

      for image_url in string.gmatch(html, '<img src="(http[^"]+)') do
        if string.match(image_url, '/photos/') then
          table.insert(urls, { url=image_url })
        end
      end

      -- scrape the usernames and tags for later
      local f = assert(io.open(item_dir .. '/twitpic2-scrape-' .. escaped_item_name .. '.txt', 'a'))

      local twitpic_id = string.match(url, "twitpic%.com/([a-z0-9]+)")
      local timestamp = string.match(html, 'short_id.-"timestamp": ?"([^"]+)"')

      f:write('timestamp:')
      f:write(twitpic_id)
      f:write(':')
      f:write(timestamp)
      f:write('\n')

      for username in string.gmatch(html, 'name="twitter:creator" value="([^"]+)"') do
        f:write('user:')
        f:write(username)
        f:write('\n')
      end

      for tag in string.gmatch(html, '"/tag/([^"]+)"') do
        f:write('tag:')
        f:write(tag)
        f:write('\n')
      end

      f:close()
    end
  end

  return urls
end


wget.callbacks.httploop_result = function(url, err, http_stat)
-- NEW for 2014: Slightly more verbose messages because people keep
-- complaining that it's not moving or not working
  local status_code = http_stat["statcode"]

  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. ".  \n")
  io.stdout:flush()

  -- consider 403 as banned from twitpic, not pernament failure
  if status_code >= 500 or
          (status_code >= 400 and status_code ~= 404 and status_code ~= 403) or
          (status_code == 403 and string.match(url["host"], "twitpic%.com")) then

    io.stdout:write("\nServer returned "..http_stat.statcode.." for " .. url["url"] .. ". Sleeping.\n")
    io.stdout:flush()

    if string.match(url["host"], "twitpic%.com") and status_code == 403 then
      io.stdout:write("\nBanned from TwitPic :(\n")
      io.stdout:flush()
      os.execute("sleep 60")
      return wget.actions.ABORT
    end

    os.execute("sleep 10")

    tries = tries + 1

    if tries >= 5 then
      tries = 0
      return wget.actions.NOTHING
    else
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  -- We're okay; sleep a bit (if we have to) and continue
  -- local sleep_time = 0.1 * (math.random(1000, 2000) / 100.0)
  local sleep_time = 0

  --  if string.match(url["host"], "cdn") or string.match(url["host"], "media") then
  --    -- We should be able to go fast on images since that's what a web browser does
  --    sleep_time = 0
  --  end

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end
