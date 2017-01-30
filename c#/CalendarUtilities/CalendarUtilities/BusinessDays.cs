using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CalendarUtilities
{
    public static class BusinessDays
    {

         public static List<DateTime> GetBusinessDays(DateTime dateFrom, DateTime dateTo)

        {
            HashSet<DateTime> holidays = new HashSet<DateTime>();
            HashSet<DateTime> businessDaysSet = new HashSet<DateTime>();

            for (int year=dateFrom.Year; year<=dateTo.Year; year++)
            {
                HashSet<DateTime> holidayList = GetHolidays(year);
                holidays.UnionWith(holidayList);
            }

            for (var dt = dateFrom; dt <= dateTo; dt = dt.AddDays(1))

            {if ((dt.DayOfWeek == DayOfWeek.Saturday) || (dt.DayOfWeek == DayOfWeek.Sunday))
                continue;
            else
                businessDaysSet.Add(dt);
            }
            
 
            List<DateTime> businessDays = businessDaysSet.Except(holidays).ToList();
            businessDays.Sort((a,b)=>a.CompareTo(b));
            return businessDays;

        }
      
        public static DateTime GetBusinessDayShifted(DateTime referanceDate, int shiftInDays)
         {

             int ShiftSign = Math.Sign(shiftInDays);
             int ShiftInDaysAbs = Math.Abs(shiftInDays);

             List<DateTime> businessDays;
             DateTime BusinessDayOut;


             int ShiftInCalDaysAbs = (int)(Math.Max(Math.Ceiling((double)ShiftInDaysAbs * 7 / 5), ShiftInDaysAbs + 8));
             int ShiftInCalDays = ShiftInCalDaysAbs * ShiftSign;

             if (ShiftSign < 0)
             {
                 businessDays = CalendarUtilities.BusinessDays.GetBusinessDays(referanceDate.AddDays(ShiftInCalDays), referanceDate);
                 businessDays.Remove(referanceDate);
                 BusinessDayOut = businessDays[businessDays.Count - ShiftInDaysAbs];
             }

             else
             {
                 businessDays = CalendarUtilities.BusinessDays.GetBusinessDays(referanceDate, referanceDate.AddDays(ShiftInCalDays));
                 businessDays.Remove(referanceDate);
                 BusinessDayOut = businessDays[shiftInDays - 1];
             }

             return BusinessDayOut;
         }

        public static DateTime GetBusinessDayShifted(int shiftInDays)
        {
            DateTime referanceDate = DateTime.Now.Date;
            return GetBusinessDayShifted(referanceDate, shiftInDays);

        }

        public static HashSet<DateTime> GetHolidays(int year)
        {
            HashSet<DateTime> holidays = new HashSet<DateTime>();
            //NEW YEARS 
            DateTime newYearsDate = AdjustForWeekendHoliday(new DateTime(year, 1, 1).Date);
            holidays.Add(newYearsDate);

            // MARTIN LUTHER KING DAY
            var martinLutherKing = (from day in Enumerable.Range(1, 31)
                                where new DateTime(year, 1, day).DayOfWeek == DayOfWeek.Monday
                                select day).ElementAt(2);
            DateTime martinLutherKingDay = new DateTime(year, 1, martinLutherKing);
            holidays.Add(martinLutherKingDay.Date);

            // US PRESIDENTS DAY --third monday of february
            var president = (from day in Enumerable.Range(1, 28)
                                    where new DateTime(year, 2, day).DayOfWeek == DayOfWeek.Monday
                                    select day).ElementAt(2);
            DateTime presidentsDay = new DateTime(year, 2, president);
            holidays.Add(presidentsDay.Date);

            // GOOD FRIDAY --two days before Easter

            DateTime goodFriday = EasterSunday(year).AddDays(-2);
            holidays.Add(goodFriday);

            //MEMORIAL DAY  -- last monday in May 
            DateTime memorialDay = new DateTime(year, 5, 31);
            DayOfWeek dayOfWeek = memorialDay.DayOfWeek;
            while (dayOfWeek != DayOfWeek.Monday)
            {
                memorialDay = memorialDay.AddDays(-1);
                dayOfWeek = memorialDay.DayOfWeek;
            }
            holidays.Add(memorialDay.Date);

            //INDEPENCENCE DAY 
            DateTime independenceDay = AdjustForWeekendHoliday(new DateTime(year, 7, 4).Date);
            holidays.Add(independenceDay);

            //LABOR DAY -- 1st Monday in September 
            DateTime laborDay = new DateTime(year, 9, 1);
            dayOfWeek = laborDay.DayOfWeek;
            while (dayOfWeek != DayOfWeek.Monday)
            {
                laborDay = laborDay.AddDays(1);
                dayOfWeek = laborDay.DayOfWeek;
            }
            holidays.Add(laborDay.Date);

            //THANKSGIVING DAY - 4th Thursday in November 
            var thanksgiving = (from day in Enumerable.Range(1, 30)
                                where new DateTime(year, 11, day).DayOfWeek == DayOfWeek.Thursday
                                select day).ElementAt(3);
            DateTime thanksgivingDay = new DateTime(year, 11, thanksgiving);
            holidays.Add(thanksgivingDay.Date);

            DateTime christmasDay = AdjustForWeekendHoliday(new DateTime(year, 12, 25).Date);
            holidays.Add(christmasDay);
            return holidays;
        }

        public static DateTime AdjustForWeekendHoliday(DateTime holiday)
        {
            if (holiday.DayOfWeek == DayOfWeek.Saturday)
            {
                return holiday.AddDays(-1);
            }
            else if (holiday.DayOfWeek == DayOfWeek.Sunday)
            {
                return holiday.AddDays(1);
            }
            else
            {
                return holiday;
            }
        }

        public static DateTime EasterSunday(int year)
        {
            int day = 0;
            int month = 0;

            int g = year % 19;
            int c = year / 100;
            int h = (c - (int)(c / 4) - (int)((8 * c + 13) / 25) + 19 * g + 15) % 30;
            int i = h - (int)(h / 28) * (1 - (int)(h / 28) * (int)(29 / (h + 1)) * (int)((21 - g) / 11));

            day = i - ((year + (int)(year / 4) + i + 2 - c + (int)(c / 4)) % 7) + 28;
            month = 3;

            if (day > 31)
            {
                month++;
                day -= 31;
            }

            return new DateTime(year, month, day);
        }


    }



    }

