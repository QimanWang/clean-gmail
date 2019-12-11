d = {'CATEGORY_PERSONAL': 3082, 'CATEGORY_SOCIAL': 5819, 'Junk': 0, 'Notes': 0, 'CATEGORY_FORUMS': 466,
     'IMPORTANT': 4568, 'wonton': 0, 'CATEGORY_UPDATES': 20550, 'CHAT': 977, 'SENT': 847, 'INBOX': 48215, 'TRASH': 0,
     'CATEGORY_PROMOTIONS': 18530, 'DRAFT': 39, 'SPAM': 5195, 'STARRED': 98, 'UNREAD': 40975}

sum = 0
for k, v in d.items():
    if 'CATEGORY' in k:
        sum+=v
        print(k,v)
print(sum)
print(d['INBOX'])
